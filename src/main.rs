use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::io;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;
use scylla::transport::errors::NewSessionError;
use scylla::{Session, SessionBuilder,FromRow};
use scylla;
use uuid::Uuid;
use scylla::frame::value::CqlTimeuuid;
use uuid::v1::{Context, Timestamp};
use std::fs::metadata;
use std::ptr::null;
use tokio::io::split;

fn open_file(path: &str) -> File {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .read(true)
        .open(path);

    file.unwrap()
}

fn read_file (path: &str) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut buffer: Vec<u8> = Vec::new();
    let _ = file.read_to_end(&mut buffer)
        .expect("Could not read file: ");

    buffer
}

fn write_file(file: &mut File, data: &[u8]) -> io::Result<()> {
    file.write_all(data)?;
    file.flush()?; // Ensure all data is written to the file
    Ok(())
}

fn delete_file(path: &str) -> std::io::Result<()> {
    fs::remove_file(path)?;
    Ok(())
}

fn file_testing(){
    let bucket_path = "./test.bucket";

    delete_file(bucket_path).expect("Could not clear the bucket file: "); //Make sure bucket is empty
    let mut file = open_file(bucket_path); //Open bucket

    let stuff = read_file("./test.jpg");
    let stuff1 = read_file("./test1.jpg");

    file.write_all(&stuff).expect("Could not write to the file: ");

    file.write_all(&stuff1).expect("Could not write to bucket");

    let _ = file.seek(SeekFrom::Start(660202));

    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf).expect("Could not read from bucket: ");

    println!("{}",buf.len());

    delete_file("./new.jpg").expect("Could not clear the new jpg file: ");

    let mut out_file = open_file("./new.jpg");
    write_file(&mut out_file,&buf).unwrap();
}

fn get_user(email: &str) {

}

async fn create_scylla_conn(scylla_uri: &str) -> Session {
    SessionBuilder::new()
        .known_node(scylla_uri)
        //.known_nodes(uri) Can add more known Scylla cluster nodes
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build()
        .await
        .unwrap()
}

fn create_timeuuid() -> CqlTimeuuid {
    let ts = Timestamp::now(Context::new(0));
    CqlTimeuuid::from(Uuid::new_v1(ts, &[1,2,3,4,5,6]))
}

#[derive(Debug, FromRow)]
struct User {
    email: String,
    created: i32,
    password: String,
    salt: String
}

#[derive(Debug, FromRow)]
struct BlockFile {
    id: CqlTimeuuid,
    filename: String,
    user: String,
    prefix: String,
}

#[derive(Debug, FromRow)]
struct FilePart {
    id: CqlTimeuuid,
    filename: String,
    sequence: i32,
    host: String,
    starting_byte: i64,
    length: i64,
}

impl Default for FilePart {
    fn default() -> Self {
        FilePart {
            id: CqlTimeuuid::from(Uuid::nil()),
            filename: "".to_string(),
            sequence: 0,
            host: "".to_string(),
            starting_byte: 0,
            length: 0
        }
    }
}

#[derive(Debug, FromRow)]
struct Host {
    hostname: String,
    ip_addr: IpAddr,
    last_heartbeat: i32
}

// For testing purposes we are going to split the files in half initially.
// Later on will support dynamic sized chunking
// Returns a vector of FilePart structs
fn split_file(path : &str, file_name: &str) -> Vec<FilePart> {

    let file_metadata = metadata(path).expect("Unable to read file metadata");
    let file_size = file_metadata.len() as i64;  // Returns file size in bytes

    let mut output: Vec<FilePart> = Vec::new();

    if file_size % 2 == 0 {

        output.push(FilePart {
            id: create_timeuuid(),
            filename: file_name.to_string(),
            sequence: 0,
            host: "localhost".to_string(),
            starting_byte: 0,
            length: file_size/2
        });

        output.push(FilePart {
            id: create_timeuuid(),
            filename: file_name.to_string(),
            sequence: 1,
            host: "localhost".to_string(),
            starting_byte: file_size/2+1,
            length: file_size/2
        });

    } else if file_size % 2 != 0 {

        output.push(FilePart {
            id: create_timeuuid(),
            filename: file_name.to_string(),
            sequence: 0,
            host: "localhost".to_string(),
            starting_byte: 0,
            length: file_size/2
        });

        output.push(FilePart {
            id: create_timeuuid(),
            filename: file_name.to_string(),
            sequence: 1,
            host: "localhost".to_string(),
            starting_byte: 0-1,
            length: file_size/2+1
        });

    }

    output

}

// Reads a section of bytes from the file
fn read_part(mut file: &File, part: FilePart) -> Vec<u8> {

    file.seek(SeekFrom::Start(part.starting_byte as u64)).unwrap();

    let mut buffer: Vec<u8> = vec![0; (part.length - 1) as usize];
    file.read_exact(&mut buffer).expect("Could not read byte section");

    buffer

}

fn upload_file( path: &str, filename: &str, session: &Session, host: String ) {

    let parts = split_file(path, "test.jpg");

    //let file = open_file(path);

    let bucket_path = "./test.bucket";

    //delete_file(bucket_path).expect("Could not clear the bucket file: "); //Make sure bucket is empty
    let mut bucket_file = open_file(bucket_path); //Open bucket

    let demo_file = "./test.jpg";
    let file_to_upload = open_file(demo_file);

    for part in parts {

        let byte_arr = read_part(&file_to_upload, part);

        bucket_file.write_all(&*byte_arr).unwrap()
        //bucket_file.seek(SeekFrom::Start(part.starting_byte as u64)).unwrap();
    }

    // file.write_all(&data).expect("Could not write to the file: ");

    //file.write_all(&data).expect("Could not write to bucket");

    //let _ = file.seek(SeekFrom::Start(660202));

    //let mut buf: Vec<u8> = Vec::new();
    //file.read_to_end(&mut buf).expect("Could not read from bucket: ");

    //println!("{}",buf.len());

    //delete_file("./new.jpg").expect("Could not clear the new jpg file: ");

    //let mut out_file = open_file("./new.jpg");
    //write_file(&mut out_file,&buf).unwrap();





}

#[tokio::main]
async fn main() {

    let uri = "10.0.0.234";
    let keyspace_name = "rusty_block";

    let session = create_scylla_conn(uri).await;

    session.use_keyspace(keyspace_name, false)
        .await.
        expect("Failed to set keyspace");


    upload_file("./test.jpg", "test.jpg", &session, "local".to_string());


    /*
    session
        .query("INSERT INTO users (email, created, password, salt) VALUES (?,?,?,?)",("john_doe@gmail.com",69420,"password","salt"))
        .await
        .expect("Failed to insert user");
    */

    /*
    for i in 0..25 {
        let timeuuid = create_timeuuid();
        session
            .query("INSERT INTO files (id,filename,prefix,fullname,user) VALUES (?,?,?,?,?)",(timeuuid, "test_file.jpg", "/", "/test_file.jpg", "rstrom1763@gmail.com"))
            .await
            .expect("Failed to insert user");
    }
    */

    /*
    let rows = session
        .query("SELECT * FROM rusty_block.files WHERE user = ? ALLOW FILTERING", ("rstrom1763@gmail.com",))
        .await
        .unwrap()
        .rows
        .unwrap();

    for row in rows {
        //let user: User = User::from_row(row).unwrap();
        println!("{:?}", row);
    }

     */

}
