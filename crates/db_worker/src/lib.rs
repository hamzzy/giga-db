use db_common::{DbError, QueryResult, Filter, FilterOperator, TableDefinition, ColumnDefinition, ColumnType, QueryPlan};
use db_storage::StorageManager;
use async_trait::async_trait;
use std::sync::Arc;
use wgpu::util::DeviceExt;
use futures::executor::block_on;
use db_sql::ExecutionTree;
#[async_trait]
pub trait Worker {
    async fn execute_query_fragment(&self, tree: &ExecutionTree) -> Result<QueryResult, DbError>;
}

pub struct GPUWorker {
    storage_manager: Arc<dyn StorageManager + Send + Sync>,
    device: wgpu::Device,
    queue: wgpu::Queue
}

impl GPUWorker {
    pub async fn new(storage_manager: Arc<dyn StorageManager + Send + Sync>) -> Result<Self, DbError> {
        let instance = wgpu::Instance::default();

    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
         .ok_or(DbError::WorkerError("No suitable graphics adapter found".to_string()))?;
        
    let (device, queue) = adapter
         .request_device(
            &wgpu::DeviceDescriptor {
                 label: None,
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::default(),
                 memory_hints: wgpu::MemoryHints::default()
             },
             None,
        )
        .await
        .map_err(|e| DbError::WorkerError(e.to_string()))?;

    Ok(Self { storage_manager, device, queue })
}
    
}

#[async_trait]
impl Worker for GPUWorker {
    async fn execute_query_fragment(&self, tree: &ExecutionTree) -> Result<QueryResult, DbError> {
         match tree {
             ExecutionTree::Select { plan} => {
                self.execute_select(plan).await
             }
         }
    }
}

impl GPUWorker {
    async fn execute_select(&self, plan: &QueryPlan) -> Result<QueryResult, DbError> {
          let table_def = TableDefinition {
             name: "test".to_string(),
             columns: vec![
               ColumnDefinition{ name: "id".to_string(), data_type: ColumnType::Integer },
               ColumnDefinition{ name: "value".to_string(), data_type: ColumnType::String },
               ]
            };
       let mut all_column_data = Vec::new();
        for chunk in &plan.chunks {
             let column_index = plan.filter.as_ref().map_or(0, |f| f.column_index);
              let column_data =  self.storage_manager.load_column_f32(
                 &table_def, &chunk.s3_key, column_index).await?;
             all_column_data.extend(column_data);
        }


        let filter_value = plan.filter.as_ref().map_or(String::from("0"), |f| f.value.clone()).parse::<f32>().unwrap_or(0.0);
           // Example GPU compute kernel for demonstration purposes
         let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor{
            label: None,
            source: wgpu::ShaderSource::Wgsl(std::borrow::Cow::Borrowed(
               r#"
                @group(0) @binding(0)
                var<storage, read_write> input_data: array<f32>;

                @group(0) @binding(1)
                var<storage, read_write> output_data: array<f32>;
                
                @group(0) @binding(2)
                var<uniform> filter_value: f32;

                @group(0) @binding(3)
                var<uniform> filter_operator: i32;
                
                @compute @workgroup_size(64)
                fn main(@builtin(global_invocation_id) id: vec3<u32>) {
                   let index = id.x;
                   
                    if filter_operator == 0 && input_data[index] > filter_value {
                        output_data[index] = input_data[index];
                    }
                    else if filter_operator == 1 && input_data[index] < filter_value {
                       output_data[index] = input_data[index];
                    } 
                    else if filter_operator == 2 && input_data[index] == filter_value {
                      output_data[index] = input_data[index];
                    }
                    else if filter_operator == 3 && input_data[index] != filter_value {
                      output_data[index] = input_data[index];
                   } else {
                        output_data[index] = 0.0;
                    }
                }
                "#
            )),
         });
            
      
         let input_buffer = self.device.create_buffer_init(
             &wgpu::util::BufferInitDescriptor {
                label: Some("Input Buffer"),
                contents: bytemuck::cast_slice(&all_column_data),
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });
       
       let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor{
        label: Some("Output Buffer"),
          size: (all_column_data.len() * std::mem::size_of::<f32>()) as wgpu::BufferAddress,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC | wgpu::BufferUsages::COPY_DST,
          mapped_at_creation: false,
       });


        let filter_buffer = self.device.create_buffer_init(
            &wgpu::util::BufferInitDescriptor {
               label: Some("Filter Buffer"),
              contents: bytemuck::cast_slice(&[filter_value]),
               usage: wgpu::BufferUsages::UNIFORM,
            });
            
        let filter_operator = plan.filter.as_ref().map_or(0, |f| match f.operator {
           FilterOperator::GreaterThan => 0,
            FilterOperator::LessThan => 1,
             FilterOperator::Equal => 2,
              FilterOperator::NotEqual => 3,
        });


        let operator_buffer = self.device.create_buffer_init(
            &wgpu::util::BufferInitDescriptor {
               label: Some("Operator Buffer"),
              contents: bytemuck::cast_slice(&[filter_operator]),
               usage: wgpu::BufferUsages::UNIFORM,
            });
       let bind_group_layout = self.device.create_bind_group_layout(
         &wgpu::BindGroupLayoutDescriptor{
           label: None,
             entries: &[
                 wgpu::BindGroupLayoutEntry {
                     binding: 0,
                     visibility: wgpu::ShaderStages::COMPUTE,
                      ty: wgpu::BindingType::Buffer {
                             ty: wgpu::BufferBindingType::Storage { read_only: false },
                              has_dynamic_offset: false,
                               min_binding_size: None,
                         },
                        count: None,
                 },
                 wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                     ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Storage { read_only: false },
                             has_dynamic_offset: false,
                              min_binding_size: None,
                        },
                       count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                     ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Uniform,
                             has_dynamic_offset: false,
                              min_binding_size: None,
                        },
                       count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::COMPUTE,
                     ty: wgpu::BindingType::Buffer {
                            ty: wgpu::BufferBindingType::Uniform,
                             has_dynamic_offset: false,
                              min_binding_size: None,
                        },
                       count: None,
                },
             ],
        });

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor{
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: input_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: output_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: filter_buffer.as_entire_binding(),
                },
                 wgpu::BindGroupEntry {
                    binding: 3,
                    resource: operator_buffer.as_entire_binding(),
                },
             ],
             label: None
        });

       let pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor{
            label: None,
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[]
        });
       let compute_pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor{
            label: None,
             layout: Some(&pipeline_layout),
             module: &shader,
              entry_point: Some("main"),
           compilation_options: Default::default(),
           cache: None
        });

       let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor{label: None});
        {
           let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor{label: None, timestamp_writes: None});
           compute_pass.set_pipeline(&compute_pipeline);
           compute_pass.set_bind_group(0, &bind_group, &[]);
           compute_pass.dispatch_workgroups(all_column_data.len() as u32 / 64 , 1, 1);
        }
        self.queue.submit(std::iter::once(encoder.finish()));


        let gpu_read_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Read Buffer"),
            size: (all_column_data.len() * std::mem::size_of::<f32>()) as wgpu::BufferAddress,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor { label: None });
        encoder.copy_buffer_to_buffer(
            &output_buffer,
            0,
            &gpu_read_buffer,
            0,
            (all_column_data.len() * std::mem::size_of::<f32>()) as wgpu::BufferAddress,
        );
        self.queue.submit(std::iter::once(encoder.finish()));

        // Fix: Create a proper future that we can block on
        let buffer_slice = gpu_read_buffer.slice(..);
        let (sender, receiver) = futures::channel::oneshot::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            let _ = sender.send(result);
        });

        // Keep the device alive until the GPU finishes
        self.device.poll(wgpu::Maintain::Wait);

        // Wait for the GPU to finish
        let map_result = block_on(receiver)
            .map_err(|e| DbError::WorkerError(format!("Failed to receive GPU result: {}", e)))?
            .map_err(|e| DbError::WorkerError(format!("Failed to map GPU buffer: {:?}", e)))?;

        // Read the results
        let filtered_values = {
            let data = buffer_slice.get_mapped_range();
            let result: Vec<f32> = data
                .chunks_exact(std::mem::size_of::<f32>())
                .map(|b| *bytemuck::from_bytes::<f32>(b))
                .collect();
            drop(data);
            gpu_read_buffer.unmap();
            result
        };

        println!("Filtered data: {:?}", filtered_values);

        // Convert filtered values to query result format
        let rows = filtered_values
            .into_iter()
            .filter(|&v| v != 0.0)  // Remove filtered out values
            .map(|v| vec![v.to_string()])
            .collect();

        Ok(QueryResult { rows })
   }
}