pub static API_KEYS: [(i32, &str); 2] = [
  (18, "APIVersions"),
  (75, "DescribeTopicPartitions")
];

pub enum ApiType {
  ApiVersions = 18,
  DTP = 75,
}

impl From<i16> for ApiType {
  fn from(v: i16) -> Self {
      match v {
          18 => ApiType::ApiVersions,
          75 => ApiType::DTP,
          _ => panic!("Unknow request type: {v}"),
      }
  }
}