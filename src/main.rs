#![allow(warnings)]

mod cli;
mod tree;


fn main() -> std::io::Result<()> {
    cli::run_cli()
}
