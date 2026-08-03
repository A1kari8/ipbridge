use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

pub const MAGIC: &[u8; 4] = b"IPBZ";
pub const HEADER_LEN: usize = 10;
pub const FLAG_COMPRESSED: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Zlib,
    Gz,
    Zstd,
}

impl Codec {
    fn id(self) -> u8 {
        match self {
            Codec::Zlib => 1,
            Codec::Gz => 2,
            Codec::Zstd => 3,
        }
    }

    fn from_id(id: u8) -> Option<Codec> {
        match id {
            1 => Some(Codec::Zlib),
            2 => Some(Codec::Gz),
            3 => Some(Codec::Zstd),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Codec::Zlib => "zlib",
            Codec::Gz => "gz",
            Codec::Zstd => "zstd",
        }
    }

    pub fn from_name(s: &str) -> Option<Codec> {
        match s {
            "zlib" => Some(Codec::Zlib),
            "gz" => Some(Codec::Gz),
            "zstd" => Some(Codec::Zstd),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compression {
    pub codec: Codec,
    pub level: i32,
}

impl Compression {
    pub fn new(codec: Codec, level: Option<i32>) -> Self {
        Self {
            codec,
            level: level.unwrap_or_else(|| default_level(codec)),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let (lo, hi) = match self.codec {
            Codec::Zlib | Codec::Gz => (1, 9),
            Codec::Zstd => (1, 22),
        };
        if self.level < lo || self.level > hi {
            return Err(format!(
                "compression level {} out of range for {} (allowed {}-{})",
                self.level,
                self.codec.name(),
                lo,
                hi
            ));
        }
        Ok(())
    }

    fn try_compress(&self, input: &[u8]) -> Option<Vec<u8>> {
        match self.codec {
            Codec::Zlib => {
                let mut enc = flate2::write::ZlibEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(self.level as u32),
                );
                enc.write_all(input).ok()?;
                enc.finish().ok()
            }
            Codec::Gz => {
                let mut enc = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(self.level as u32),
                );
                enc.write_all(input).ok()?;
                enc.finish().ok()
            }
            Codec::Zstd => zstd::bulk::compress(input, self.level).ok(),
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            codec: Codec::Zlib,
            level: default_level(Codec::Zlib),
        }
    }
}

fn default_level(codec: Codec) -> i32 {
    match codec {
        Codec::Zlib | Codec::Gz => 6,
        Codec::Zstd => 3,
    }
}

fn decompress_payload(codec: Codec, data: &[u8], cap: usize) -> Option<Vec<u8>> {
    match codec {
        Codec::Zlib => {
            let mut dec = flate2::read::ZlibDecoder::new(data).take(cap as u64);
            let mut out = Vec::with_capacity(cap);
            dec.read_to_end(&mut out).ok()?;
            Some(out)
        }
        Codec::Gz => {
            let mut dec = flate2::read::GzDecoder::new(data).take(cap as u64);
            let mut out = Vec::with_capacity(cap);
            dec.read_to_end(&mut out).ok()?;
            Some(out)
        }
        Codec::Zstd => zstd::bulk::decompress(data, cap).ok(),
    }
}

fn write_header(
    out: &mut Vec<u8>,
    codec: Codec,
    compressed: bool,
    orig_len: usize,
    pay_len: usize,
) {
    out.extend_from_slice(MAGIC);
    out.push(if compressed { FLAG_COMPRESSED } else { 0 });
    out.push(codec.id());
    out.extend_from_slice(&(orig_len as u16).to_le_bytes());
    out.extend_from_slice(&(pay_len as u16).to_le_bytes());
}

/// 压缩数据块（UDP/TCP 通用）。恒带魔数头以支持接收端自动识别解压；
/// 压缩无收益（小包/已压缩数据）时存为原始字节，不会越压越大。
pub fn compress_frame(c: Compression, input: &[u8], out: &mut Vec<u8>) {
    out.clear();
    if input.len() > u16::MAX as usize {
        write_header(out, c.codec, false, input.len(), input.len());
        out.extend_from_slice(input);
        return;
    }
    if let Some(compressed) = c.try_compress(input)
        && compressed.len() + HEADER_LEN < input.len()
    {
        write_header(out, c.codec, true, input.len(), compressed.len());
        out.extend_from_slice(&compressed);
        return;
    }
    write_header(out, c.codec, false, input.len(), input.len());
    out.extend_from_slice(input);
}

/// 尝试解压。`input` 以魔数开头时解压/还原并返回输出长度，否则返回 `None`（透传）。
pub fn decompress(input: &[u8], out: &mut [u8]) -> Option<usize> {
    if input.len() < HEADER_LEN || &input[..4] != MAGIC {
        return None;
    }
    let codec = Codec::from_id(input[5])?;
    let compressed = input[4] & FLAG_COMPRESSED != 0;
    let orig_len = u16::from_le_bytes([input[6], input[7]]) as usize;
    let pay_len = u16::from_le_bytes([input[8], input[9]]) as usize;
    if orig_len > out.len() || pay_len > input.len() - HEADER_LEN {
        return None;
    }
    let payload = &input[HEADER_LEN..HEADER_LEN + pay_len];
    if !compressed {
        if orig_len != pay_len {
            return None;
        }
        out[..orig_len].copy_from_slice(payload);
        return Some(orig_len);
    }
    let data = decompress_payload(codec, payload, orig_len)?;
    if data.len() != orig_len {
        return None;
    }
    out[..orig_len].copy_from_slice(&data);
    Some(orig_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repetitive(len: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(len);
        while v.len() < len {
            v.extend_from_slice(b"abc123xyz789");
        }
        v.truncate(len);
        v
    }

    #[test]
    fn roundtrip_all_codecs() {
        for codec in [Codec::Zlib, Codec::Gz, Codec::Zstd] {
            let data = repetitive(4096);
            let mut frame = Vec::new();
            compress_frame(Compression::new(codec, None), &data, &mut frame);
            assert_eq!(&frame[..4], MAGIC);
            let mut out = vec![0u8; BUF_SIZE_REF];
            let n = decompress(&frame, &mut out).unwrap();
            assert_eq!(n, data.len());
            assert_eq!(&out[..n], &data[..]);
        }
    }

    const BUF_SIZE_REF: usize = 65_535;

    #[test]
    fn store_fallback_for_incompressible() {
        let mut data = vec![0u8; 512];
        let mut state: u64 = 0x9E3779B97F4A7C15;
        for b in data.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let mut frame = Vec::new();
        compress_frame(Compression::default(), &data, &mut frame);
        assert_eq!(&frame[..4], MAGIC, "store fallback must still be framed");
        let mut out = vec![0u8; BUF_SIZE_REF];
        let n = decompress(&frame, &mut out).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&out[..n], &data[..]);
    }

    #[test]
    fn frame_always_has_magic() {
        let data = repetitive(64);
        let mut frame = Vec::new();
        compress_frame(Compression::default(), &data, &mut frame);
        assert_eq!(&frame[..4], MAGIC);
        let mut out = vec![0u8; BUF_SIZE_REF];
        let n = decompress(&frame, &mut out).unwrap();
        assert_eq!(&out[..n], &data[..]);
    }

    #[test]
    fn decompress_none_for_non_magic() {
        let mut out = vec![0u8; 64];
        assert_eq!(decompress(b"hello world", &mut out), None);
    }

    #[test]
    fn decompress_rejects_bad_length() {
        let data = repetitive(100);
        let mut frame = Vec::new();
        compress_frame(Compression::default(), &data, &mut frame);
        frame[7] = 0xFF;
        let mut out = vec![0u8; BUF_SIZE_REF];
        assert_eq!(decompress(&frame, &mut out), None);
    }

    #[test]
    fn decompress_rejects_oversized_output() {
        let data = repetitive(1000);
        let mut frame = Vec::new();
        compress_frame(Compression::default(), &data, &mut frame);
        frame[6] = 0xFF;
        frame[7] = 0xFF;
        let mut out = vec![0u8; 512];
        assert_eq!(decompress(&frame, &mut out), None);
    }

    #[test]
    fn stored_frame_roundtrip() {
        let data = repetitive(100);
        let mut frame = Vec::new();
        compress_frame(Compression::new(Codec::Zstd, None), &data, &mut frame);
        let mut out = vec![0u8; BUF_SIZE_REF];
        let n = decompress(&frame, &mut out).unwrap();
        assert_eq!(&out[..n], &data[..]);
    }

    #[test]
    fn level_defaults_per_codec() {
        assert_eq!(Compression::new(Codec::Zlib, None).level, 6);
        assert_eq!(Compression::new(Codec::Gz, None).level, 6);
        assert_eq!(Compression::new(Codec::Zstd, None).level, 3);
    }

    #[test]
    fn level_validation() {
        assert!(Compression::new(Codec::Zlib, Some(9)).validate().is_ok());
        assert!(Compression::new(Codec::Zlib, Some(10)).validate().is_err());
        assert!(Compression::new(Codec::Zstd, Some(22)).validate().is_ok());
        assert!(Compression::new(Codec::Zstd, Some(23)).validate().is_err());
        assert!(Compression::new(Codec::Zstd, Some(0)).validate().is_err());
    }

    #[test]
    fn codec_name_roundtrip() {
        for codec in [Codec::Zlib, Codec::Gz, Codec::Zstd] {
            assert_eq!(Codec::from_name(codec.name()), Some(codec));
        }
        assert_eq!(Codec::from_name("lz4"), None);
    }

    #[test]
    fn level_affects_ratio() {
        let data = repetitive(4096);
        let low = Compression::new(Codec::Zlib, Some(1));
        let high = Compression::new(Codec::Zlib, Some(9));
        let mut f1 = Vec::new();
        let mut f9 = Vec::new();
        compress_frame(low, &data, &mut f1);
        compress_frame(high, &data, &mut f9);
        assert!(f9.len() <= f1.len(), "higher level should not be worse");
    }
}
