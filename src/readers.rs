use std::{error::Error, io::Read};

pub(crate) fn read_u8(reader: &mut impl Read) -> Result<u8, Box<dyn Error>> {
    let mut buffer: [u8; 1] = [0; 1];
    reader.read_exact(&mut buffer)?;
    Ok(buffer[0])
}

pub(crate) fn read_u16(reader: &mut impl Read) -> Result<u16, Box<dyn Error>> {
    let mut buffer: [u8; 2] = [0; 2];
    reader.read_exact(&mut buffer)?;
    Ok(u16::from_be_bytes(buffer))
}

pub(crate) fn read_u32(reader: &mut impl Read) -> Result<u32, Box<dyn Error>> {
    let mut buffer: [u8; 4] = [0; 4];
    reader.read_exact(&mut buffer)?;
    Ok(u32::from_be_bytes(buffer))
}

pub(crate) fn read_bytes(length: usize, reader: &mut impl Read) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![0; length];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

pub(crate) fn read_string(reader: &mut impl Read) -> Result<String, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![];
    loop {
        let mut byte: [u8; 1] = [0; 1];
        reader.read_exact(&mut byte)?;
        if byte[0] == 0 {
            break;
        }
        buffer.push(byte[0]);
    }
    Ok(String::from_utf8(buffer)?)
}
