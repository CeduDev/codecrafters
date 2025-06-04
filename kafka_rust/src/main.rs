#![allow(unused_imports)]
use std::any::Any;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};

use anyhow::Result;
use bytes::{Buf, BytesMut};


mod kafka;
use kafka::requests::{
    ApiVersionRequest,
    AllRequests,
    DTPRequest,
};
use kafka::responses::{
    ApiVersionsResponse,
    UnsupportedVersionResponse,
    ApiVersion,
    ApiVersionResponses,
    AllResponses,
    DTPResponse,
    DTPResponseBody,
    DTPResponseBodyTopic
};
use kafka::common::{
    API_KEYS,
    ApiType,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    fn do_dtp_request(request: DTPRequest) -> anyhow::Result<DTPResponse> {
        let topic_name_length = request.topics.first().unwrap().name_length;
        let topic_name = request.topics.first().unwrap().name.clone();
        let mut res_vec : Vec<u8> = vec![];
        for i in 0..(topic_name_length - 1 )as usize {
            res_vec.push(topic_name[i] as u8);
        }

        // println!("request topic name: {}", String::from_utf8(res_vec.clone()).expect("Invalid UTF-8"));

        return Ok(DTPResponse {
            // Message size comes from response header + response body
            message: 55_i32.to_be_bytes(),
            correlation_id: request.correlation_id.to_be_bytes(),
            tag_buffer: 0_i8.to_be_bytes(),
            response_body: DTPResponseBody {
                throttle_time: 0_i32.to_be_bytes(),
                topic_arr_length: 2_i8.to_be_bytes(),
                topic: DTPResponseBodyTopic {
                    error_code: 3_i16.to_be_bytes(),
                    topic_name_length: topic_name_length.to_be_bytes(),
                    topic_name: res_vec,
                    topic_id: 0_i128.to_be_bytes(),
                    is_internal: 0_i8.to_be_bytes(),
                    partitions_array: 1_i8.to_be_bytes(),
                    topic_authorized_operations: [0, 0, 13, 248],
                    tag_buffer: 0_i8.to_be_bytes()
                },
                next_cursor: request.cursor.to_be_bytes(),
                tag_buffer: 0_i8.to_be_bytes()
            }
        })
    }

    fn do_api_version_request(request: ApiVersionRequest) -> anyhow::Result<ApiVersionResponses> {
        const UNSUPPORTED_VERSION: i16 = 35;

        if API_KEYS.iter().any(|tuple: &(i32, &str)|  tuple.0 == request.request_api_key as i32 ) {
            let version = request.request_api_version;
            let correlation_id = request.correlation_id;

            if version >= 0 && version <= 4 {
                return Ok(ApiVersionResponses::ApiVersionsResponse( ApiVersionsResponse {
                    // Message size is derived from the byte sizes of the remaining element, see struct definition for the breakdown
                    message: 26_i32.to_be_bytes(),
                    correlation_id: correlation_id.to_be_bytes(),
                    error_code: 0_i16.to_be_bytes(),
                    api_keys_len: 3_i8.to_be_bytes(),
                    api_versions: [
                        // Version 18
                        ApiVersion {
                            api_key: (API_KEYS[0].0 as i16).to_be_bytes(),
                            min_version: 0_i16.to_be_bytes(),
                            max_version: 4_i16.to_be_bytes(),
                            tag_buffer: 0_i8.to_be_bytes(),
                        },
                        // Version 75
                        ApiVersion {
                            api_key: (API_KEYS[1].0 as i16).to_be_bytes(),
                            min_version: 0_i16.to_be_bytes(),
                            max_version: 0_i16.to_be_bytes(),
                            tag_buffer: 0_i8.to_be_bytes(),                    
                        }
                    ],
                    throttle_time_ms: 0_i32.to_be_bytes(),
                    tag_buffer_end: 0_i8.to_be_bytes()
                }));
            } else {
                return Ok(ApiVersionResponses::UnsupportedVersionResponse( UnsupportedVersionResponse {
                    message: 10_i32.to_be_bytes(),
                    correlation_id: correlation_id.to_be_bytes(),
                    error_code: UNSUPPORTED_VERSION.to_be_bytes()
                }));
            }
        } else {
            println!("Invalid API_VERSION_KEY");
            panic!("help");
        }
    }
    
    fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
        // Setup
        loop {
            let mut buffer: [u8; 1024] = [0; 1024];

            let bytes_read = match stream.read(&mut buffer) {
                Ok(0) => {
                    println!("Connection closed by client");
                    return Ok(());
                }
                Ok(n) => n,
                Err(e) => return Err(anyhow::anyhow!("Failed to read from stream: {}", e)),
            };

            let buf: BytesMut = bytes::BytesMut::from(&buffer[..bytes_read]);
            let request: AllRequests = AllRequests::from_bytes(buf).unwrap();

            let response: AllResponses = match request {
                AllRequests::ApiVersionRequest(api_request) => {
                    println!("process ApiVersions");
                    match do_api_version_request(api_request) {
                        Ok(a) => kafka::responses::AllResponses::ApiVersionResponses(a),
                        Err(e) => {
                            println!("Failed to process ApiVersions");
                            println!("Error: {:?}", e);
                            panic!("HELP");
                        }
                    }
                }
                
                AllRequests::DTPRequest(dtp_request) => {
                    println!("process DescribeTopicPartitions");
                    let mut res = match do_dtp_request(dtp_request) {
                        Ok(a) => AllResponses::DTPResponse(a),
                        Err(e) => {
                            println!("Failed to process DescribeTopicPartitions");
                            println!("Error: {:?}", e);
                            panic!("HELP");
                        }
                    };

                    if let AllResponses::DTPResponse(ref mut dtp_response) = res {
                        // Now you can call the method specific to DTPResponse

                        // Is it for a single topic?
                        // if dtp_response.response_body.topic.topic_name_length[0] > 0 {
                        let topic_id = dtp_response.topic_exists_in_log()?;
                        dtp_response.response_body.topic.error_code = 0_i16.to_be_bytes();
                        dtp_response.response_body.topic.topic_id = topic_id;
                        println!("dtp_response: {:?}", dtp_response);
                        // }
                    }

                    res
                }
            };
            
            // println!("Response: {:?}", response);
            stream.write_all(&response.clone().get_vec())?;
            // println!("Normal response: {:?}", response.get_vec());
        }    
    }


    // Uncomment this block to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}
