#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = geodatafusion_fuzz::invoke_wkt(data);
});
