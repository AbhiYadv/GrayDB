//! COPY text-format line parsing: the bridge from SP1's staged backfill parts to
//! store rows. Reverses COPY TO escaping so backfill values byte-match the raw
//! renderings pgoutput delivers for the same data (Type Interpretation Contract v0).

/// Parse one COPY text line into column values. `\N` (unquoted) = NULL.
pub fn parse_copy_line(line: &[u8]) -> Vec<Option<String>> {
    line.split(|&b| b == b'\t').map(unescape_field).collect()
}

fn unescape_field(field: &[u8]) -> Option<String> {
    if field == b"\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        let b = field[i];
        if b == b'\\' && i + 1 < field.len() {
            i += 1;
            out.push(match field[i] {
                b'b' => 0x08,
                b'f' => 0x0C,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'v' => 0x0B,
                b'\\' => b'\\',
                other => other, // unknown escape: keep the char (COPY never emits these)
            });
        } else {
            out.push(b);
        }
        i += 1;
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_fields_and_null() {
        let row = parse_copy_line(b"42\thello\t\\N\t");
        assert_eq!(
            row,
            vec![
                Some("42".into()),
                Some("hello".into()),
                None,
                Some("".into())
            ]
        );
    }

    #[test]
    fn unescapes_specials() {
        let row = parse_copy_line(b"a\\tb\\nc\\\\d");
        assert_eq!(row, vec![Some("a\tb\nc\\d".into())]);
    }
}
