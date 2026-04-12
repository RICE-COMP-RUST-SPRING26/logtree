#![allow(warnings)]

mod tree;
mod cli;


fn main() -> std::io::Result<()> {
    cli::run_cli()
}
