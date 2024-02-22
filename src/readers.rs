use std::{error::Error, io::Read};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

pub(crate) fn read_u8(reader: &mut impl Read) -> Result<u8, Box<dyn Error>> {
    let mut buffer: [u8; 1] = [0; 1];
    reader.read_exact(&mut buffer)?;
    Ok(buffer[0])
}

pub(crate) async fn read_u8_async(reader: &mut (impl AsyncRead + Unpin)) -> Result<u8, Box<dyn Error>> {
    Ok(reader.read_u8().await?)
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

pub(crate) async fn read_u32_async(reader: &mut (impl AsyncRead + Unpin)) -> Result<u32, Box<dyn Error>> {
    Ok(reader.read_u32().await?)
}

pub(crate) fn read_bytes(length: usize, reader: &mut impl Read) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![0; length];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

pub(crate) async fn read_bytes_async<R: AsyncRead + Unpin>(length: usize, reader: &mut BufReader<R>) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buffer: Vec<u8> = vec![0; length];
    reader.read_exact(&mut buffer).await?;
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
