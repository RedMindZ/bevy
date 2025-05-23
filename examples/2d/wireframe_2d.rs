//! Showcases wireframe rendering for 2d meshes.
//!
//! Wireframes currently do not work when using webgl or webgpu.
//! Supported platforms:
//! - DX12
//! - Vulkan
//! - Metal
//!
//! This is a native only feature.

use bevy::{
    color::palettes::basic::{GREEN, RED, WHITE},
    prelude::*,
    render::{
        render_resource::WgpuFeatures,
        settings::{RenderCreation, WgpuSettings},
        RenderPlugin,
    },
    sprite::{NoWireframe2d, Wireframe2d, Wireframe2dColor, Wireframe2dConfig, Wireframe2dPlugin},
};

fn main() {
    App::new()
        .add_plugins((
            DefaultPlugins.set(RenderPlugin {
                render_creation: RenderCreation::Automatic(WgpuSettings {
                    // WARN this is a native only feature. It will not work with webgl or webgpu
                    features: WgpuFeatures::POLYGON_MODE_LINE,
                    ..default()
                }),
                ..default()
            }),
            // You need to add this plugin to enable wireframe rendering
            Wireframe2dPlugin::default(),
        ))
        // Wireframes can be configured with this resource. This can be changed at runtime.
        .insert_resource(Wireframe2dConfig {
            // The global wireframe config enables drawing of wireframes on every mesh,
            // except those with `NoWireframe2d`. Meshes with `Wireframe2d` will always have a wireframe,
            // regardless of the global configuration.
            global: true,
            // Controls the default color of all wireframes. Used as the default color for global wireframes.
            // Can be changed per mesh using the `Wireframe2dColor` component.
            default_color: WHITE.into(),
        })
        .add_systems(Startup, setup)
        .add_systems(Update, update_colors)
        .run();
}

/// Set up a simple 3D scene
fn setup(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
) {
    // Triangle: Never renders a wireframe
    commands.spawn((
        Mesh2d(meshes.add(Triangle2d::new(
            Vec2::new(0.0, 50.0),
            Vec2::new(-50.0, -50.0),
            Vec2::new(50.0, -50.0),
        ))),
        MeshMaterial2d(materials.add(Color::BLACK)),
        Transform::from_xyz(-150.0, 0.0, 0.0),
        NoWireframe2d,
    ));
    // Rectangle: Follows global wireframe setting
    commands.spawn((
        Mesh2d(meshes.add(Rectangle::new(100.0, 100.0))),
        MeshMaterial2d(materials.add(Color::BLACK)),
        Transform::from_xyz(0.0, 0.0, 0.0),
    ));
    // Circle: Always renders a wireframe
    commands.spawn((
        Mesh2d(meshes.add(Circle::new(50.0))),
        MeshMaterial2d(materials.add(Color::BLACK)),
        Transform::from_xyz(150.0, 0.0, 0.0),
        Wireframe2d,
        // This lets you configure the wireframe color of this entity.
        // If not set, this will use the color in `WireframeConfig`
        Wireframe2dColor {
            color: GREEN.into(),
        },
    ));

    commands.spawn(Camera2d);

    // Text used to show controls
    commands.spawn((
        Text::default(),
        Node {
            position_type: PositionType::Absolute,
            top: Val::Px(12.0),
            left: Val::Px(12.0),
            ..default()
        },
    ));
}

/// This system lets you toggle various wireframe settings
fn update_colors(
    keyboard_input: Res<ButtonInput<KeyCode>>,
    mut config: ResMut<Wireframe2dConfig>,
    mut wireframe_colors: Query<&mut Wireframe2dColor>,
    mut text: Single<&mut Text>,
) {
    text.0 = format!(
        "Controls
---------------
Z - Toggle global
X - Change global color
C - Change color of the circle wireframe

Wireframe2dConfig
-------------
Global: {}
Color: {:?}",
        config.global,
        config.default_color.to_srgba(),
    );

    // Toggle showing a wireframe on all meshes
    if keyboard_input.just_pressed(KeyCode::KeyZ) {
        config.global = !config.global;
    }

    // Toggle the global wireframe color
    if keyboard_input.just_pressed(KeyCode::KeyX) {
        config.default_color = if config.default_color == WHITE.into() {
            RED.into()
        } else {
            WHITE.into()
        };
    }

    // Toggle the color of a wireframe using `Wireframe2dColor` and not the global color
    if keyboard_input.just_pressed(KeyCode::KeyC) {
        for mut color in &mut wireframe_colors {
            color.color = if color.color == GREEN.into() {
                RED.into()
            } else {
                GREEN.into()
            };
        }
    }
}
