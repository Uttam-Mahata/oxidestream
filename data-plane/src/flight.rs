use std::pin::Pin;
use futures::Stream;
use tonic::{Request, Response, Status, Streaming};
use arrow_flight::{
    flight_service_server::FlightService,
    FlightData, Ticket, Action, ActionType, Criteria, Empty, FlightDescriptor,
    FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    PollInfo,
};
use serde::{Deserialize, Serialize};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use std::io::{Read, Seek};
use tokio_stream::StreamExt;
use futures::TryStreamExt;

#[derive(Serialize, Deserialize, Debug)]
pub struct ShuffleTicket {
    pub stage_id: String,
    pub task_id: String,
    pub partition_id: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartitionRange {
    pub offset: u64,
    pub length: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ShuffleIndex {
    pub partitions: std::collections::HashMap<String, PartitionRange>,
}

pub struct ShuffleFlightServer {
    pub data_dir: std::path::PathBuf,
}

#[tonic::async_trait]
impl FlightService for ShuffleFlightServer {
    type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn handshake(&self, _request: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn list_flights(&self, _request: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn get_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn poll_flight_info(&self, _request: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn get_schema(&self, _request: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket_bytes = request.into_inner().ticket;
        let ticket: ShuffleTicket = serde_json::from_slice(&ticket_bytes)
            .map_err(|e| Status::invalid_argument(format!("Failed to parse ticket JSON: {}", e)))?;

        // Push-Based Shuffling retrieval path (fetch pre-merged file)
        let merged_file_name = format!("merged_shuffle_{}_{}.arrow", ticket.stage_id, ticket.partition_id);
        let merged_file_path = self.data_dir.join(&merged_file_name);

        if merged_file_path.exists() {
            let file = std::fs::File::open(&merged_file_path)
                .map_err(|e| Status::not_found(format!("Merged shuffle file not found at {:?}: {}", merged_file_path, e)))?;
            let reader = FileReader::try_new(file, None)
                .map_err(|e| Status::internal(format!("Failed to create IPC reader: {}", e)))?;
            let schema = reader.schema();
            let mut batches = Vec::new();
            for batch_res in reader {
                let batch = batch_res.map_err(|e| Status::internal(format!("Failed to read batch: {}", e)))?;
                batches.push(batch);
            }
            let flight_data_list = arrow_flight::utils::batches_to_flight_data(&schema, batches)
                .map_err(|e| Status::internal(format!("Failed to convert batches to flight data: {}", e)))?;
            let stream = tokio_stream::iter(flight_data_list.into_iter().map(Ok));
            return Ok(Response::new(Box::pin(stream) as Self::DoGetStream));
        }

        // Pull-Based / Index-Based retrieval path
        let index_file_name = format!("shuffle_{}_{}.index", ticket.stage_id, ticket.task_id);
        let index_file_path = self.data_dir.join(&index_file_name);
        
        let index_bytes = std::fs::read(&index_file_path)
            .map_err(|e| Status::not_found(format!("Index file not found at {:?}: {}", index_file_path, e)))?;
        
        let index: ShuffleIndex = serde_json::from_slice(&index_bytes)
            .map_err(|e| Status::internal(format!("Failed to parse index JSON: {}", e)))?;
            
        let part_range = index.partitions.get(&ticket.partition_id.to_string())
            .ok_or_else(|| Status::not_found(format!("Partition {} not found in index", ticket.partition_id)))?;

        let arrow_file_name = format!("shuffle_{}_{}.arrow", ticket.stage_id, ticket.task_id);
        let arrow_file_path = self.data_dir.join(&arrow_file_name);

        let mut file = std::fs::File::open(&arrow_file_path)
            .map_err(|e| Status::not_found(format!("Shuffle file not found at {:?}: {}", arrow_file_path, e)))?;

        file.seek(std::io::SeekFrom::Start(part_range.offset))
            .map_err(|e| Status::internal(format!("Failed to seek arrow file: {}", e)))?;

        if part_range.length == 0 {
            let stream = tokio_stream::iter(std::iter::empty());
            return Ok(Response::new(Box::pin(stream) as Self::DoGetStream));
        }

        let take_reader = file.take(part_range.length);

        let reader = FileReader::try_new(take_reader, None)
            .map_err(|e| Status::internal(format!("Failed to create IPC reader: {}", e)))?;

        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch_res in reader {
            let batch = batch_res.map_err(|e| Status::internal(format!("Failed to read batch: {}", e)))?;
            batches.push(batch);
        }

        let flight_data_list = arrow_flight::utils::batches_to_flight_data(&schema, batches)
            .map_err(|e| Status::internal(format!("Failed to convert batches to flight data: {}", e)))?;

        let stream = tokio_stream::iter(flight_data_list.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::DoGetStream))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        
        let first_msg = stream.next().await
            .ok_or_else(|| Status::invalid_argument("Empty stream"))??;
            
        let (stage_id, partition_id) = {
            let descriptor = first_msg.flight_descriptor.as_ref()
                .ok_or_else(|| Status::invalid_argument("Missing FlightDescriptor"))?;
                
            if descriptor.path.len() < 2 {
                return Err(Status::invalid_argument("FlightDescriptor path must contain stage_id and partition_id"));
            }
            (descriptor.path[0].clone(), descriptor.path[1].clone())
        };
        
        let prepended_stream = tokio_stream::once(Ok(first_msg)).chain(stream);
        let arrow_stream = prepended_stream.map_err(arrow_flight::error::FlightError::from);
        let mut record_batch_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(arrow_stream);
        
        let mut batches = Vec::new();
        let mut schema = None;
        while let Some(batch_res) = record_batch_stream.next().await {
            let batch = batch_res.map_err(|e| Status::internal(e.to_string()))?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }
        
        if let Some(schema) = schema {
            let file_name = format!("merged_shuffle_{}_{}.arrow", stage_id, partition_id);
            let file_path = self.data_dir.join(&file_name);
            
            let mut all_batches = Vec::new();
            if file_path.exists() {
                if let Ok(file) = std::fs::File::open(&file_path) {
                    if let Ok(reader) = FileReader::try_new(file, None) {
                        for b in reader {
                            if let Ok(b) = b {
                                all_batches.push(b);
                            }
                        }
                    }
                }
            }
            all_batches.extend(batches);
            
            let file = std::fs::File::create(&file_path)
                .map_err(|e| Status::internal(format!("Failed to create merged file: {}", e)))?;
            let mut writer = FileWriter::try_new(file, &schema)
                .map_err(|e| Status::internal(format!("Failed to create writer: {}", e)))?;
            for batch in all_batches {
                writer.write(&batch)
                    .map_err(|e| Status::internal(format!("Failed to write batch: {}", e)))?;
            }
            writer.finish()
                .map_err(|e| Status::internal(format!("Failed to finish writer: {}", e)))?;
        }
        
        let out_stream = tokio_stream::iter(std::iter::empty());
        Ok(Response::new(Box::pin(out_stream) as Self::DoPutStream))
    }

    async fn do_action(&self, _request: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn list_actions(&self, _request: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
    async fn do_exchange(&self, _request: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
