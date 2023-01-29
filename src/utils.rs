use csv::{self};
use rayon::prelude::*;
use std::{
    fs::File,
    io::{prelude::*, self},
    path::Path,
    time::Instant,
};
use indicatif::{ProgressBar, ProgressStyle};

pub fn calculate_buffer_size(file_size: u64) -> usize {
    let buffer_size = if file_size < (1 << 20) {
        8192
    } else if file_size < (1 << 30) {
        1 << 20
    } else {
        1 << 30
    };
    buffer_size as usize
}

pub fn split_file(
    input_file: &Path,
    total_rows: usize,
    thread_count: usize,
    max_lines_per_chunk: usize,
) {

    if !std::path::Path::new("chunks").exists() {
        std::fs::create_dir("chunks").unwrap();
    }

    println!("Splitting {} into {} chunks with {} threads", input_file.display(), max_lines_per_chunk, thread_count);

    let chunk_count: usize = (total_rows as f64 / max_lines_per_chunk as f64).ceil() as usize; // calculate the number of chunks (round up)

    let name = input_file.file_stem().unwrap().to_str().unwrap(); // get the name of the file

    for chunk in 0..chunk_count {

        println!("Splitting ({}/{})", chunk, chunk_count);
        let chunk_timer = Instant::now();

        let progress_bar = ProgressBar::new(thread_count as u64);
        
        progress_bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] ({pos}/{len}, ETA {eta})",
            ).unwrap(),
        );

        (0..thread_count).into_par_iter().for_each(|subchunk_id| {
            let start = chunk * max_lines_per_chunk + subchunk_id * (max_lines_per_chunk / thread_count); // calculate the start of the chunk
            let end = std::cmp::min(start + max_lines_per_chunk / thread_count, total_rows); // calculate the end of the chunk
            
            let mut reader = csv::Reader::from_path(input_file).unwrap();
            let mut writer = csv::Writer::from_path(format!("chunks/{}_{}.csv", name, subchunk_id)).unwrap();
            let mut count = 0;
            let records = reader.records();
            
            for result in records {
                let record = result.unwrap();

                // write the header
                if count == 0 {
                    writer.write_record(&record).unwrap();
                } else if count >= start && count < end {
                    writer.write_record(&record).unwrap();
                }
                count += 1;
                if count >= end {
                    break;
                }
            }
            writer.flush().unwrap();
            progress_bar.inc(1);
        });
        progress_bar.finish();

        let mut chunk_result = File::create(format!("{}_{}.csv", name, chunk)).unwrap();

        for subchunk_id in 0..thread_count {
            let mut subchunk = File::open(format!("chunks/{}_{}.csv", name, subchunk_id)).unwrap();
            io::copy(&mut subchunk, &mut chunk_result).unwrap();
        }
        chunk_result.flush().unwrap();

        println!("Took {}s", chunk_timer.elapsed().as_secs());
    }
}

pub fn count_lines(path: &Path, buffer_size: usize) -> usize {
    let timer = Instant::now();
    let mut file = File::open(path).unwrap();
    let mut buffer = vec![0; buffer_size];
    let mut total_lines = 0;
    let mut bytes_read;

    println!("Counting lines w/ {} bytes", buffer_size);

    loop {
        bytes_read = file.read(&mut buffer).unwrap();

        if bytes_read == 0 {
            break;
        }

        let buffer_str = match std::str::from_utf8(&buffer[..bytes_read]) {
            Ok(s) => s,
            Err(e) => {
                let valid_up_to = e.valid_up_to();
                let valid_bytes = &buffer[..valid_up_to];
                let valid_str = std::str::from_utf8(valid_bytes).unwrap();
                total_lines += valid_str.matches('\n').count();
                continue;
            }
        };
        total_lines += buffer_str.matches('\n').count();
    }

    println!("Took {}s", timer.elapsed().as_secs());
    total_lines
}