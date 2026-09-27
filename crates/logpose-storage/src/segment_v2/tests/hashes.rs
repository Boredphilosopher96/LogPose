//! The hashes and checksums the format depends on, pinned to reference
//! values.
//!
//! The golden file pins only the hash inputs that happen to occur in it (a
//! few short string keys and one schema snapshot), and `xxh3_64` takes a
//! different code path for each input length class. These vectors come from
//! the reference C implementations (`xxhash` 0.8 and `crc32c`), so a
//! dependency upgrade that changes any output fails here instead of silently
//! breaking every `PkFilter` and `schema_hash` on disk.

use crate::segment_v2::{canonical_pk_hash, format::crc};
use logpose_types::record::PrimaryKey;
use twox_hash::XxHash3_64;

/// The xxHash sanity buffer: `byte[i] = (gen >> 56)`, `gen = PRIME32 *
/// PRIME64^i` (wrapping).
fn sanity_buffer(len: usize) -> Vec<u8> {
    const PRIME32: u64 = 2_654_435_761;
    const PRIME64: u64 = 11_400_714_785_074_694_797;
    let mut generator = PRIME32;
    (0..len)
        .map(|_| {
            let byte = (generator >> 56) as u8;
            generator = generator.wrapping_mul(PRIME64);
            byte
        })
        .collect()
}

#[test]
fn xxh3_64_matches_the_reference_vectors_for_every_length_class() {
    // (length, XXH3_64bits(sanity_buffer[..length], seed 0)) from xxHash's
    // sanity test vectors: every length class and block boundary.
    let vectors: [(usize, u64); 13] = [
        (0, 0x2D06_8005_38D3_94C2),
        (1, 0xC44B_DFF4_074E_ECDB),
        (6, 0x27B5_6A84_CD2D_7325),
        (12, 0xA713_DAF0_DFBB_77E7),
        (24, 0xA3FE_70BF_9D35_10EB),
        (48, 0x397D_A259_ECBA_1F11),
        (80, 0xBCDE_FBBB_2C47_C90A),
        (195, 0xCD94_217E_E362_EC3A),
        (403, 0xCDEB_804D_65C6_DEA4),
        (512, 0x617E_4959_9013_CB6B),
        (2048, 0xDD59_E2C3_A5F0_38E0),
        (2240, 0x6E73_A905_39CF_2948),
        (2367, 0xCB37_AEB9_E5D3_61ED),
    ];
    let buffer = sanity_buffer(2367);
    for (len, expected) in vectors {
        assert_eq!(
            XxHash3_64::oneshot(&buffer[..len]),
            expected,
            "xxh3_64 of {len} bytes"
        );
    }
}

#[test]
fn canonical_pk_hash_matches_the_reference() {
    // xxh3_64 of 0x01 ++ i64 LE and 0x02 ++ UTF-8, computed with the
    // reference implementation.
    let vectors = [
        (PrimaryKey::Int64(i64::MIN), 0x6F16_315A_EE31_3307),
        (PrimaryKey::Int64(0), 0x313C_2DD2_EC99_6E80),
        (PrimaryKey::Int64(1), 0xA048_B5CB_8216_2577),
        (PrimaryKey::Int64(i64::MAX), 0xBA0E_0DA8_D48B_9D86),
        (PrimaryKey::String(String::new()), 0xC9F4_2E6C_9E93_DFFF),
        (PrimaryKey::String("a".to_owned()), 0x9134_6750_400E_332E),
        (PrimaryKey::String("A-1".to_owned()), 0x08C0_359F_752D_D9CB),
        (
            PrimaryKey::String("na\u{ef}ve".to_owned()),
            0x449C_4192_8890_6BE9,
        ),
        (PrimaryKey::String("x".repeat(300)), 0x380F_A685_5E75_DBE3),
    ];
    for (pk, expected) in vectors {
        assert_eq!(canonical_pk_hash(&pk), expected, "hash of {pk:?}");
    }
}

#[test]
fn crc32c_matches_the_check_value() {
    assert_eq!(crc(b"123456789"), 0xE306_9283);
    assert_eq!(crc(b""), 0);
}
