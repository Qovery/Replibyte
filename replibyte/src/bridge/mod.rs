use crate::connector::Connector;

pub mod s3;

pub trait Bridge: Connector {}
