// Protocol definitions and validation for various LLM APIs
// This module provides a structured approach to handling different API protocols

pub mod spec;
pub mod validation;
pub mod worker_spec;

pub(crate) mod routing_text;

pub(crate) mod lmdeploy;
