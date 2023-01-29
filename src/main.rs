use std::{time::Instant, fs::File};
mod utils;

const HELP_MESSAGE: &str = "Usage: split <file> <max_lines_per_chunk> <thread_count>
file is the path to the csv file to merge
chunk_size is the number of rows per chunk
thread_count is the number of threads to use
";

fn main() {

    let timer: Instant = Instant::now();
    let args: Vec<String> = std::env::args().collect::<Vec<String>>();
    
    if args.len() != 4 {
        println!("{}", HELP_MESSAGE);
        std::process::exit(1);
    }

    // Get the path to the file
    let path = std::path::Path::new(&args[1]);
    
    // split into files that have x lines
    let max_lines_per_chunk : usize = args[2].parse::<usize>().unwrap();
    
    // number of threads to use
    let thread_count: usize = args[3].parse::<usize>().unwrap();

    // calculate the buffer size
    let buffer: usize = {
        let file: File = std::fs::File::open(path).unwrap();
        let file_size: u64 = file.metadata().unwrap().len();
        let buffer_size: usize = utils::calculate_buffer_size(file_size);
        buffer_size
    };

    // count the number of lines in the file
    let total_rows = utils::count_lines(path, buffer);

    // split the file into chunks
    utils::split_file(path, total_rows, thread_count, max_lines_per_chunk);
    
    println!("Finished in {}s", timer.elapsed().as_secs());

}