use anyhow::Result;
use bytes::{Buf, BytesMut};

#[derive(Debug, Clone)]
pub enum MetadataComponents {
  MetadataLogFile(MetadataLogFile),
  Record(Record),
  TopicRecord(TopicRecord)
}

#[derive(Debug, Clone, Default)]
pub struct TopicRecord {
  pub frame_version: u8,
  pub type_: u8,
  pub version: u8,
  pub name_length: u8,
  pub name: Vec<u8>,
  pub topic_uuid: u128,
  pub tagged_fields_count: u8,
}

#[derive(Debug, Clone, Default)]
pub struct Record {
  pub length: u8,
  pub attributes: u8,
  pub timestamp_delta: u8,
  pub offset_delta: u8,
  pub key_length: u8,
  pub value_length: u8,
  pub topic_records: Vec<TopicRecord>,
}

#[derive(Debug, Clone, Default)]
pub struct MetadataLogFile {
  pub base_offset: u64,
  pub batch_length: u32,
  pub partition_leader_epoch: u32,
  pub magic_byte: u8,
  pub crc: u32,
  pub attributes: u16,
  pub last_offset_delta: u32,
  pub base_timestamp: u64,
  pub max_timestamp: u64,
  pub producer_id: u64,
  pub producer_epoch: u16,
  pub base_sequence: u32,
  pub records_length: u32,
  pub records: Vec<Record>,
  pub headers_array_count: u8
}

impl MetadataLogFile {
  pub fn from_bytes(mut input: BytesMut) -> Result<MetadataLogFile> {
    let base_offset = input.get_u64();
    println!("base_offset: {}", base_offset);
    let batch_length = input.get_u32();
    println!("batch_length: {}", batch_length);
    if base_offset == (1 as u64) {
      input.get_uint(batch_length as usize);
      // input.
    }
    println!("batch_length: {}", batch_length);
    let partition_leader_epoch = input.get_u32();
    let magic_byte = input.get_u8();
    let crc = input.get_u32();
    let attributes = input.get_u16();
    let last_offset_delta = input.get_u32();
    let base_timestamp = input.get_u64();
    let max_timestamp = input.get_u64();
    let producer_id = input.get_u64();
    let producer_epoch = input.get_u16();
    let base_sequence = input.get_u32();

    let records_length = input.get_u32();
    let mut records: Vec<Record> = vec![];
    for _ in 0..records_length {
      let length = input.get_u8();
      println!("!!!!!!!!!!length: {}", length);
      let attributes = input.get_u8();
      let timestamp_delta = input.get_u8();
      let offset_delta = input.get_u8();
      let key_length = input.get_u8();
      if key_length >= (2 as u8) {
        todo!("shouldn't reach here for now!");
      }

      let value_length = input.get_u8();
      println!("!!!!!!!!!!value_length: {} (should be like lenght - 6)", value_length);
      let mut topic_records: Vec<TopicRecord> = vec![];
      // println!("from_bytes, lenght of feature level records: {}", value_length);
      // for _ in 0..value_length {
        let frame_version = input.get_u8();
        let type_ = input.get_u8();

        println!("type: {}", type_);
        
        let version = input.get_u8();
        let name_length = input.get_u8();
        let name = (0..(name_length - 1) as usize)
          .map(|_| input.get_u8())
          .collect::<Vec<u8>>();
        let topic_uuid: u128 = input.get_u128();
        println!("name: {:?}", String::from_utf8(name.clone()).expect("Invalid UTF-8"));
        let tagged_fields_count = input.get_u8();
        // if type_ == (12 as u8) {
          let jee = TopicRecord { 
            frame_version: frame_version,
            type_: type_,
            version: version,
            name_length: name_length,
            name: name,
            topic_uuid: topic_uuid,
            tagged_fields_count: tagged_fields_count 
          };

          // println!("topic record: {:?}", jee);

          topic_records.push(jee);
        // }

      // }

      records.push(Record { length ,
        attributes: attributes ,
        timestamp_delta: timestamp_delta ,
        offset_delta: offset_delta ,
        key_length: key_length ,
        value_length: value_length ,
        topic_records: topic_records  
      });
    }
    
    let headers_array_count = input.get_u8();

    Ok(MetadataLogFile { 
      base_offset: base_offset,
      batch_length: batch_length,
      partition_leader_epoch: partition_leader_epoch,
      magic_byte: magic_byte,
      crc: crc,
      attributes: attributes,
      last_offset_delta: last_offset_delta,
      base_timestamp: base_timestamp,
      max_timestamp: max_timestamp,
      producer_id: producer_id,
      producer_epoch: producer_epoch,
      base_sequence: base_sequence,
      records_length: records_length,
      records: records,
      headers_array_count: headers_array_count, 
    })    
  }
}