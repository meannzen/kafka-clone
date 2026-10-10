use std::io::Cursor;

use codecrafters_kafka::{
    protocol::{self, Request},
    storage::{ClusterMetadata, cluster_metadata::MetadataRecord},
};

const APPLE_ID: [u8; 16] = [
    113, 165, 154, 81, 137, 104, 79, 139, 147, 126, 62, 78, 57, 43, 189, 214,
];

fn metadata() -> ClusterMetadata {
    ClusterMetadata::from_bytes(include_bytes!("../00000000000000000000.log")).unwrap()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn respond(raw: &[u8]) -> Vec<u8> {
    let request = Request::parse(&mut Cursor::new(raw)).unwrap();
    protocol::respond(&request, &metadata()).unwrap()
}

fn request(api_key: i16, api_version: i16, correlation_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![0, 9];
    body.extend(b"kafka-cli");
    body.push(0);
    body.extend(payload);

    let mut msg = ((body.len() + 8) as i32).to_be_bytes().to_vec();
    msg.extend(api_key.to_be_bytes());
    msg.extend(api_version.to_be_bytes());
    msg.extend(correlation_id.to_be_bytes());
    msg.extend(body);
    msg
}

#[test]
fn parses_cluster_metadata_log() {
    let metadata = metadata();
    assert_eq!(metadata.records.len(), 3);
    assert!(matches!(
        &metadata.records[0],
        MetadataRecord::FeatureLevel(f) if f.name == "metadata.version" && f.feature_level == 20
    ));

    let topic = metadata.find_topic("apple").unwrap();
    assert_eq!(topic.topic_id, APPLE_ID);
    assert_eq!(
        metadata.find_topic_by_id(&APPLE_ID).unwrap().topic_name,
        "apple"
    );

    let partitions = metadata.find_partitions(&APPLE_ID);
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].leader, 1);
    assert_eq!(partitions[0].replicas, vec![1]);
}

#[test]
fn api_versions() {
    assert_eq!(
        respond(&[0, 0, 0, 8, 0, 18, 0, 4, 0, 0, 0, 9]),
        unhex("0000000900000500000000000b0000120000000400004b0000000000000100000010000000000000")
    );
    assert_eq!(
        respond(&[0, 0, 0, 8, 0, 18, 0, 5, 0, 0, 0, 9]),
        unhex("0000000900230500000000000b0000120000000400004b0000000000000100000010000000000000")
    );
}

#[test]
fn describe_unknown_topic() {
    assert_eq!(
        respond(include_bytes!("../kafka_request.bin")),
        unhex("00000007000000000002000304666f6f0000000000000000000000000000000000010000000000ff00")
    );
}

#[test]
fn describe_known_topic() {
    let mut payload = vec![2, 6];
    payload.extend(b"apple");
    payload.extend([0, 0, 0, 0, 0x64, 0xff, 0]);
    assert_eq!(
        respond(&request(75, 0, 8, &payload)),
        unhex(&[
            "000000080000000000020000066170706c6571a59a5189684f8b937e3e4e392bbdd600020000000000000000",
            "000100000000020000000102000000010101010000000df800ff00",
        ]
        .concat())
    );
}

#[test]
fn fetch_known_and_unknown_topic() {
    let raw = unhex(&[
        "00000093000100100000000700096b61666b612d636c6900000001f400000001000003e80000000000000000",
        "000371a59a5189684f8b937e3e4e392bbdd60200000000000000000000000000000000000000000000000000",
        "000000000000000000abababababababababababababababab02000000000000000000000000000000000000",
        "00000000000000000000000000000000010100",
    ]
    .concat());
    assert_eq!(
        respond(&raw),
        unhex(&[
            "0000000700000000000000000000000371a59a5189684f8b937e3e4e392bbdd6020000000000000000000000",
            "0000000000000000000000000000000000000001ffffffff010000abababababababababababababababab02",
            "00000000006400000000000000000000000000000000000000000000000001ffffffff01000000",
        ]
        .concat())
    );
}
