use std::{fs::File, io::{self, BufRead, BufReader, Read}, path::Path};

use anyhow::Result;
use bytes::{Buf, BytesMut};

use crate::kafka::metadata_log_file::{TopicRecord, MetadataLogFile, Record};
use std::ops::ControlFlow::Break;

use super::metadata_log_file::MetadataComponents;

#[derive(Debug, Clone)]
pub enum AllResponses {
  ApiVersionResponses(ApiVersionResponses),
  DTPResponse(DTPResponse)
}

impl AllResponses {
  pub fn get_vec(self) -> Vec<u8> {
    match self {
      AllResponses::ApiVersionResponses(resp) => resp.get_vec(),
      AllResponses::DTPResponse(resp) => resp.get_vec()
    }
  }
}

#[derive(Debug, Clone, Copy)]
pub enum ApiVersionResponses {
  ApiVersionsResponse(ApiVersionsResponse),
  UnsupportedVersionResponse(UnsupportedVersionResponse)
}

impl ApiVersionResponses {
  pub fn get_vec(self) -> Vec<u8> {
      match self {
          ApiVersionResponses::ApiVersionsResponse(resp) => resp.get_vec(),
          ApiVersionResponses::UnsupportedVersionResponse(resp) => resp.get_vec(),
      }
  }
}

#[derive(Debug, Copy, Clone)]
pub struct ApiVersion {
  pub api_key: [u8; 2],
  pub min_version: [u8; 2],
  pub max_version: [u8; 2],
  pub tag_buffer: [u8; 1],
}

#[derive(Debug, Copy, Clone)]
pub struct ApiVersionsResponse<> {
  pub message: [u8; 4],
  pub correlation_id: [u8; 4],
  pub error_code: [u8; 2],
  pub api_keys_len: [u8; 1],
  pub api_versions: [ApiVersion; 2],
  pub throttle_time_ms: [u8; 4],
  pub tag_buffer_end: [u8; 1]
}

impl ApiVersionsResponse {
  pub fn get_vec(self) -> Vec<u8> {
    let mut buf = vec![];

    // Header
    buf.extend_from_slice(&self.message);
    buf.extend_from_slice(&self.correlation_id);
    buf.extend_from_slice(&self.error_code);
    buf.extend_from_slice(&self.api_keys_len);
    
    for version in &self.api_versions {
      buf.extend_from_slice(&version.api_key);
      buf.extend_from_slice(&version.min_version);
      buf.extend_from_slice(&version.max_version);
      buf.extend_from_slice(&version.tag_buffer);
    }

    buf.extend_from_slice(&self.throttle_time_ms);
    buf.extend_from_slice(&self.tag_buffer_end);

    buf
  }
}

#[derive(Debug, Clone)]
pub struct DTPResponseBodyTopic {
  pub error_code: [u8; 2],
  pub topic_name_length: [u8; 1],
  pub topic_name: Vec<u8>,
  pub topic_id: [u8; 16],
  pub is_internal: [u8; 1],
  pub partitions_array: [u8; 1],
  pub topic_authorized_operations: [u8; 4],
  pub tag_buffer: [u8; 1]
}

#[derive(Debug, Clone)]
pub struct DTPResponseBody {
  pub throttle_time: [u8; 4],
  pub topic_arr_length: [u8; 1],
  pub topic: DTPResponseBodyTopic,
  pub next_cursor: [u8; 1],
  pub tag_buffer: [u8; 1],
}

#[derive(Debug, Clone)]
pub struct DTPResponse {
  pub message: [u8; 4],
  pub correlation_id: [u8; 4],
  pub tag_buffer: [u8; 1],
  pub response_body: DTPResponseBody
}


impl DTPResponse {
  pub fn get_vec(&self) -> Vec<u8> { 
    let mut buf = vec![];
    let body = &self.response_body;
    let topic = &self.response_body.topic;

    // Header
    buf.extend_from_slice(&self.message);
    buf.extend_from_slice(&self.correlation_id);
    buf.extend_from_slice(&self.tag_buffer);
    
    // Body
    buf.extend_from_slice(&body.throttle_time);
    buf.extend_from_slice(&body.topic_arr_length);
    
    // Topic
    buf.extend_from_slice(&topic.error_code);
    buf.extend_from_slice(&topic.topic_name_length);
    buf.extend_from_slice(&topic.topic_name);
    buf.extend_from_slice(&topic.topic_id);
    buf.extend_from_slice(&topic.is_internal);
    buf.extend_from_slice(&topic.partitions_array);
    buf.extend_from_slice(&topic.topic_authorized_operations);
    buf.extend_from_slice(&topic.tag_buffer);

    // Body again
    buf.extend_from_slice(&body.next_cursor);
    buf.extend_from_slice(&body.tag_buffer);

    buf
  }

  pub fn helper(&self, log_file: &MetadataLogFile) -> [u8; 16] {
    let binding = String::from_utf8(self.response_body.topic.topic_name.clone()).expect("Invalid UTF-8");
    let topic_name_as_string: &str = binding.as_str();

    println!("Number of records: {}", log_file.records.len());

    let mut res: [u8; 16] = 0_i128.to_be_bytes();
    log_file.records.iter().for_each(|record| {
      println!("do iter stuff: {:?}", record.topic_records);
      println!("on this amount of topic records: {:?}", record.value_length);
      return record.topic_records.iter().for_each(|tr| {
        let tr_name_binding = String::from_utf8(tr.name.clone()).expect("Invalid UTF-8");
        let tr_name = tr_name_binding.as_str();
        println!("tr name: {:?}", tr_name);
        println!("topic_name_as_string: {:?}", topic_name_as_string);
        // if tr_name == topic_name_as_string {
          res = tr.topic_uuid.to_be_bytes();
        // }
      })
    });

    if u128::from_be_bytes(res) == 0 as u128 {
      println!("DIDN'T FIND A CORRECT TopicRecord WITH GIVEN NAME: {:?}!!!!", topic_name_as_string);
    }

    res
  }
  
  pub fn topic_exists_in_log(&self) -> anyhow::Result<[u8; 16]> {
    let mut file = File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")?;
    let mut buffer: [u8; 1024] = [0; 1024];

    let bytes_read = match file.read(&mut buffer) {
      Ok(0) => {
          println!("Log file has 0 bytes!");
          0
      }
      Ok(n) => n,
      Err(_) => panic!("ups"),
    };

    let buf: BytesMut = bytes::BytesMut::from(&buffer[..bytes_read]);

    println!("Read file with n: {:?} bytes!", bytes_read);

    let log_file: MetadataLogFile = MetadataLogFile::from_bytes(buf).unwrap();
    println!("Log file: {:?}", log_file);

    Ok(self.helper(&log_file))
  }
}
  
#[derive(Debug, Copy, Clone)]
pub struct UnsupportedVersionResponse {
  pub message: [u8; 4],
  pub correlation_id: [u8; 4],
  pub error_code: [u8; 2]
}

impl UnsupportedVersionResponse {
  pub fn get_vec(self) -> Vec<u8> {
      return [
          self.message.as_slice(),
          self.correlation_id.as_slice(),
          self.error_code.as_slice(),
      ].concat();
  }
}