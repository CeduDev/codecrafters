use anyhow::Result;
use bytes::{Buf, BytesMut};

pub enum AllRequests {
  ApiVersionRequest(ApiVersionRequest),
  DTPRequest(DTPRequest)
}

impl AllRequests {
  pub fn from_bytes(input: BytesMut) -> Result<AllRequests> {
    let mut peek = input.clone(); // Clone input so we don't consume the actual buffer

    let _message_size = peek.get_i32(); // skip size
    let api_key = peek.get_i16();       // this tells us which request type it is

    match api_key {
        18 => {
            // ApiVersions
            let request = ApiVersionRequest::from_bytes(input)?;
            Ok(AllRequests::ApiVersionRequest(request))
        }
        75 => {
            // DTP
            let request = DTPRequest::from_bytes(input)?;
            Ok(AllRequests::DTPRequest(request))
        }
        _ => Err(anyhow::anyhow!("Unsupported API key: {}", api_key)),
    }
  }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ApiVersionRequest {
    pub message_size: i32,
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32
}

impl ApiVersionRequest {
  pub fn from_bytes(mut input: BytesMut) -> Result<ApiVersionRequest> {
      let message_size = input.get_i32();
      let request_api_key = input.get_i16();
      let request_api_version = input.get_i16();
      let correlation_id = input.get_i32();
      Ok(ApiVersionRequest {
          message_size,
          request_api_key,
          request_api_version,
          correlation_id
      })
  }
}

#[derive(Debug)]
pub struct DTPTopic {
    pub name_length: i8,
    pub name: Vec<i8>,
    pub tag_buffer: i8,
}

pub struct DTPRequest {
  pub message_size: i32,
  pub request_api_key: i16,
  pub request_api_version: i16,
  pub correlation_id: i32,
  pub client_length: i16,
  pub client_content: Vec<i8>,
  pub tag_buffer: i8,
  pub topic_array_length: i8,
  pub topics: Vec<DTPTopic>,
  pub response_partition_limit: i32,
  pub cursor: i8,
  pub tag_buffer_end: i8
}

impl DTPRequest {
  pub fn from_bytes(mut input: BytesMut) -> Result<DTPRequest> {
    let message_size = input.get_i32();
    let request_api_key = input.get_i16();
    let request_api_version = input.get_i16();
    let correlation_id = input.get_i32();

    let client_length = input.get_i16();
    let client_content = (0..client_length as usize)
      .map(|_| input.get_i8())
      .collect::<Vec<i8>>();
    let tag_buffer = input.get_i8();

    let mut topics: Vec<DTPTopic> = vec![];
    let topic_array_length = input.get_i8();
    for _ in 0..topic_array_length - 1 {
      let topic_len = input.get_i8();
      let name = (0..(topic_len-1) as usize)
        .map(|_| input.get_i8())
        .collect::<Vec<i8>>();
      let tag_buffer = input.get_i8();
      topics.push({DTPTopic{name_length: topic_len, name, tag_buffer}});
    }

    let response_partition_limit = input.get_i32();
    let cursor = input.get_i8();
    let tag_buffer_end = input.get_i8();

    Ok(DTPRequest {
      message_size,
      request_api_key,
      request_api_version,
      correlation_id,
      client_length, 
      client_content,
      tag_buffer,
      topic_array_length,
      topics,
      response_partition_limit,
      cursor,
      tag_buffer_end,
    })
  }
}