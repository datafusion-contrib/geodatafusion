//! A small, non-expanding XML tree used by geometry fragment parsers.

use datafusion::error::{DataFusionError, Result};
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::{Namespace, ResolveResult};
use quick_xml::reader::NsReader;

const MAX_XML_BYTES: usize = 16 * 1024 * 1024;
const MAX_XML_DEPTH: usize = 256;
const MAX_XML_ELEMENTS: usize = 100_000;

#[derive(Debug, Clone)]
pub(crate) struct XmlNode {
    pub(crate) name: String,
    pub(crate) attributes: Vec<XmlAttribute>,
    pub(crate) children: Vec<XmlNode>,
    pub(crate) text: String,
}

#[derive(Debug, Clone)]
pub(crate) struct XmlAttribute {
    pub(crate) name: String,
    pub(crate) value: String,
}

impl XmlNode {
    pub(crate) fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|attribute| attribute.name == name)
            .map(|attribute| attribute.value.as_str())
    }

    pub(crate) fn child(&self, name: &str) -> Option<&XmlNode> {
        self.children.iter().find(|child| child.name == name)
    }

    pub(crate) fn trimmed_text(&self) -> &str {
        self.text.trim()
    }
}

pub(crate) fn parse(
    input: &str,
    allowed_namespaces: &[&str],
    format_name: &str,
) -> Result<XmlNode> {
    if input.len() > MAX_XML_BYTES {
        return Err(invalid(
            format_name,
            format!("XML input exceeds {MAX_XML_BYTES} bytes"),
        ));
    }

    let mut reader = NsReader::from_str(input);
    reader.config_mut().enable_all_checks(true);

    let mut stack = Vec::<XmlNode>::new();
    let mut root = None;
    let mut element_count = 0usize;

    loop {
        let event = reader
            .read_event()
            .map_err(|error| invalid(format_name, format!("malformed XML: {error}")))?;

        match event {
            Event::Start(start) => {
                let namespace = resolved_namespace(
                    reader.resolver().resolve_element(start.name()).0,
                    allowed_namespaces,
                    format_name,
                )?;
                let node = start_node(
                    &reader,
                    &start,
                    namespace,
                    allowed_namespaces,
                    format_name,
                    &mut element_count,
                )?;
                if stack.len() >= MAX_XML_DEPTH {
                    return Err(invalid(
                        format_name,
                        format!("XML nesting exceeds {MAX_XML_DEPTH} elements"),
                    ));
                }
                stack.push(node);
            }
            Event::Empty(start) => {
                let namespace = resolved_namespace(
                    reader.resolver().resolve_element(start.name()).0,
                    allowed_namespaces,
                    format_name,
                )?;
                let node = start_node(
                    &reader,
                    &start,
                    namespace,
                    allowed_namespaces,
                    format_name,
                    &mut element_count,
                )?;
                attach_node(node, &mut stack, &mut root, format_name)?;
            }
            Event::End(_) => {
                let node = stack
                    .pop()
                    .ok_or_else(|| invalid(format_name, "XML has an unmatched closing element"))?;
                attach_node(node, &mut stack, &mut root, format_name)?;
            }
            Event::Text(text) => {
                let decoded = text
                    .xml10_content()
                    .map_err(|error| invalid(format_name, format!("invalid XML text: {error}")))?;
                append_text(&mut stack, &decoded, &mut root, format_name)?;
            }
            Event::CData(cdata) => {
                let decoded = cdata
                    .decode()
                    .map_err(|error| invalid(format_name, format!("invalid CDATA: {error}")))?;
                append_text(&mut stack, &decoded, &mut root, format_name)?;
            }
            Event::GeneralRef(reference) => {
                let value = resolve_reference(&reference, format_name)?;
                append_text(&mut stack, &value.to_string(), &mut root, format_name)?;
            }
            Event::DocType(_) => {
                return Err(invalid(
                    format_name,
                    "DOCTYPE declarations and entity definitions are not allowed",
                ));
            }
            Event::Decl(_) | Event::PI(_) | Event::Comment(_) => {}
            Event::Eof => break,
        }
    }

    if !stack.is_empty() {
        return Err(invalid(format_name, "XML document is truncated"));
    }
    root.ok_or_else(|| invalid(format_name, "XML document has no root element"))
}

fn start_node(
    reader: &NsReader<&[u8]>,
    start: &BytesStart<'_>,
    _namespace: Option<String>,
    allowed_namespaces: &[&str],
    format_name: &str,
    element_count: &mut usize,
) -> Result<XmlNode> {
    *element_count += 1;
    if *element_count > MAX_XML_ELEMENTS {
        return Err(invalid(
            format_name,
            format!("XML document exceeds {MAX_XML_ELEMENTS} elements"),
        ));
    }

    let name = decode_qname(start.local_name().as_ref(), format_name)?;
    let mut attributes = Vec::new();

    for attribute in start.attributes().with_checks(true) {
        let attribute = attribute
            .map_err(|error| invalid(format_name, format!("invalid XML attribute: {error}")))?;
        let key = attribute.key;
        if key.as_ref() == b"xmlns" || key.as_ref().starts_with(b"xmlns:") {
            continue;
        }

        let (attribute_resolution, local_name) = reader.resolver().resolve_attribute(key);
        let _attribute_namespace =
            resolved_namespace(attribute_resolution, allowed_namespaces, format_name)?;
        let name = decode_qname(local_name.as_ref(), format_name)?;
        let value = attribute
            .decode_and_unescape_value(reader.decoder())
            .map_err(|error| {
                invalid(format_name, format!("invalid XML attribute value: {error}"))
            })?;
        attributes.push(XmlAttribute {
            name,
            value: value.into_owned(),
        });
    }

    Ok(XmlNode {
        name,
        attributes,
        children: Vec::new(),
        text: String::new(),
    })
}

fn resolved_namespace(
    resolution: ResolveResult<'_>,
    allowed_namespaces: &[&str],
    format_name: &str,
) -> Result<Option<String>> {
    match resolution {
        ResolveResult::Bound(Namespace(namespace)) => {
            let namespace = std::str::from_utf8(namespace)
                .map_err(|_| invalid(format_name, "XML namespace is not valid UTF-8"))?;
            if !allowed_namespaces.contains(&namespace) {
                return Err(invalid(
                    format_name,
                    format!("unsupported XML namespace {namespace:?}"),
                ));
            }
            Ok(Some(namespace.to_string()))
        }
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Unknown(prefix) => Err(invalid(
            format_name,
            format!(
                "unbound XML namespace prefix {:?}",
                String::from_utf8_lossy(prefix.as_ref())
            ),
        )),
    }
}

fn decode_qname(name: &[u8], format_name: &str) -> Result<String> {
    std::str::from_utf8(name)
        .map(str::to_string)
        .map_err(|_| invalid(format_name, "XML element name is not valid UTF-8"))
}

fn attach_node(
    node: XmlNode,
    stack: &mut [XmlNode],
    root: &mut Option<XmlNode>,
    format_name: &str,
) -> Result<()> {
    if let Some(parent) = stack.last_mut() {
        parent.children.push(node);
    } else if root.replace(node).is_some() {
        return Err(invalid(
            format_name,
            "XML document contains more than one root element",
        ));
    }
    Ok(())
}

fn append_text(
    stack: &mut [XmlNode],
    text: &str,
    root: &mut Option<XmlNode>,
    format_name: &str,
) -> Result<()> {
    if let Some(current) = stack.last_mut() {
        current.text.push_str(text);
    } else if !text.trim().is_empty() {
        if root.is_some() {
            return Err(invalid(
                format_name,
                "XML document contains trailing text after its root element",
            ));
        }
        return Err(invalid(
            format_name,
            "XML document contains text before its root element",
        ));
    }
    Ok(())
}

fn resolve_reference(
    reference: &quick_xml::events::BytesRef<'_>,
    format_name: &str,
) -> Result<char> {
    if let Some(character) = reference.resolve_char_ref().map_err(|error| {
        invalid(
            format_name,
            format!("invalid XML character reference: {error}"),
        )
    })? {
        return Ok(character);
    }

    let decoded = reference.decode().map_err(|error| {
        invalid(
            format_name,
            format!("invalid XML entity reference: {error}"),
        )
    })?;
    match decoded.as_ref() {
        "amp" => Ok('&'),
        "apos" => Ok('\''),
        "gt" => Ok('>'),
        "lt" => Ok('<'),
        "quot" => Ok('"'),
        name => Err(invalid(
            format_name,
            format!("custom XML entity &{name}; is not allowed"),
        )),
    }
}

pub(crate) fn invalid(format_name: &str, message: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("invalid {format_name} representation: {message}"))
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_namespaced_fragment() {
        let node = parse(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos>1 2</gml:pos></gml:Point>"#,
            &["http://www.opengis.net/gml/3.2"],
            "GML",
        )
        .unwrap();
        assert_eq!(node.name, "Point");
        assert_eq!(node.child("pos").unwrap().trimmed_text(), "1 2");
    }

    #[test]
    fn rejects_doctype() {
        let error = parse(
            r#"<!DOCTYPE gml:Point [<!ENTITY x "1">]><Point>&x; 2</Point>"#,
            &["http://www.opengis.net/gml"],
            "GML",
        )
        .unwrap_err();
        assert!(error.to_string().contains("DOCTYPE"));
    }

    #[test]
    fn rejects_wrong_namespace() {
        let error = parse(
            r#"<gml:Point xmlns:gml="urn:not-gml"><gml:pos>1 2</gml:pos></gml:Point>"#,
            &["http://www.opengis.net/gml"],
            "GML",
        )
        .unwrap_err();
        assert!(error.to_string().contains("unsupported XML namespace"));
    }
}
