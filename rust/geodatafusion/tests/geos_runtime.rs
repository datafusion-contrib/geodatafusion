//! Runtime checks for the GEOS version declared by the enabled feature.

#[cfg(feature = "geos-3_11")]
mod tests {
    use std::env;

    #[test]
    fn geos_runtime_satisfies_declared_minimum() {
        let version = geos::version().expect("failed to read the GEOS runtime version");
        let mut parts = version.split('.');
        let major: u32 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));
        let minor: u32 = parts
            .next()
            .and_then(|part| part.parse().ok())
            .unwrap_or_else(|| panic!("unexpected GEOS version `{version}`"));

        let (required_major, required_minor) = if cfg!(feature = "geos-3_12") {
            (3, 12)
        } else {
            (3, 11)
        };
        assert!(
            major > required_major || (major == required_major && minor >= required_minor),
            "enabled GEOS features require GEOS >= {required_major}.{required_minor}, got {version}"
        );

        if let Ok(expected) = env::var("GEODATAFUSION_EXPECT_GEOS_VERSION") {
            let version_prefix = version.split('-').next().unwrap_or(version.as_str());
            assert_eq!(
                version_prefix,
                expected.trim(),
                "the loaded GEOS runtime does not match the requested minimum-version lane"
            );
        }

        println!("GEOS runtime version: {version}");
    }
}
