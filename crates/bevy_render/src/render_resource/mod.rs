mod batched_uniform_buffer;
mod bind_group;
mod bind_group_entries;
mod bind_group_layout;
mod bind_group_layout_entries;
mod bindless;
mod buffer;
mod buffer_vec;
mod gpu_array_buffer;
mod pipeline;
mod pipeline_cache;
mod pipeline_specializer;
pub mod resource_macros;
mod shader;
mod storage_buffer;
mod texture;
mod uniform_buffer;

pub use bind_group::*;
pub use bind_group_entries::*;
pub use bind_group_layout::*;
pub use bind_group_layout_entries::*;
pub use bindless::*;
pub use buffer::*;
pub use buffer_vec::*;
pub use gpu_array_buffer::*;
pub use pipeline::*;
pub use pipeline_cache::*;
pub use pipeline_specializer::*;
pub use shader::*;
pub use storage_buffer::*;
pub use texture::*;
pub use uniform_buffer::*;

// TODO: decide where re-exports should go
pub use wgpu::{
    util::{
        BufferInitDescriptor, DispatchIndirectArgs, DrawIndexedIndirectArgs, DrawIndirectArgs,
        TextureDataOrder,
    },
    AdapterInfo as WgpuAdapterInfo, AddressMode, AstcBlock, AstcChannel, BindGroupDescriptor,
    BindGroupEntry, BindGroupLayoutDescriptor, BindGroupLayoutEntry, BindingResource, BindingType,
    BlendComponent, BlendFactor, BlendOperation, BlendState, BufferAddress, BufferAsyncError,
    BufferBinding, BufferBindingType, BufferDescriptor, BufferSize, BufferUsages, ColorTargetState,
    ColorWrites, CommandEncoder, CommandEncoderDescriptor, CompareFunction, ComputePass,
    ComputePassDescriptor, ComputePipelineDescriptor as RawComputePipelineDescriptor,
    DepthBiasState, DepthStencilState, DownlevelFlags, Extent3d, Face, Features as WgpuFeatures,
    FilterMode, FragmentState as RawFragmentState, FrontFace, ImageSubresourceRange, IndexFormat,
    Limits as WgpuLimits, LoadOp, MapMode, MultisampleState, Operations, Origin3d,
    PipelineCompilationOptions, PipelineLayout, PipelineLayoutDescriptor, PollError, PollStatus,
    PollType, PolygonMode, PrimitiveState, PrimitiveTopology, PushConstantRange,
    RenderPassColorAttachment, RenderPassDepthStencilAttachment, RenderPassDescriptor,
    RenderPipelineDescriptor as RawRenderPipelineDescriptor, Sampler as WgpuSampler,
    SamplerBindingType, SamplerBindingType as WgpuSamplerBindingType, SamplerBorderColor,
    SamplerDescriptor, ShaderModule, ShaderModuleDescriptor, ShaderSource, ShaderStages,
    StencilFaceState, StencilOperation, StencilState, StorageTextureAccess, StoreOp,
    TexelCopyBufferInfo, TexelCopyBufferLayout, TexelCopyTextureInfo, TextureAspect,
    TextureDescriptor, TextureDimension, TextureFormat, TextureFormatFeatureFlags,
    TextureFormatFeatures, TextureSampleType, TextureUsages, TextureView as WgpuTextureView,
    TextureViewDescriptor, TextureViewDimension, VertexAttribute,
    VertexBufferLayout as RawVertexBufferLayout, VertexFormat, VertexState as RawVertexState,
    VertexStepMode, COPY_BUFFER_ALIGNMENT, COPY_BYTES_PER_ROW_ALIGNMENT,
};

pub use crate::mesh::VertexBufferLayout;

pub mod encase {
    pub use bevy_encase_derive::ShaderType;
    pub use encase::*;
}

pub use self::encase::{ShaderSize, ShaderType};

pub use naga::ShaderStage;
