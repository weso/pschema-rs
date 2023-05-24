#!/bin/bash
env POLARS_MAX_THREADS=16
cargo run -r --example benchmark