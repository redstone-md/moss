import * as THREE from "three";
import { Time, mulberry32 } from "../core.js";
import { FOG_GLSL } from "./Network.js";

// Instanced moss/grass field with Crytek-style triangle-wave wind.
// The literal moss under the digital mesh — the name made physical.

const bladeVert = /* glsl */ `
  attribute vec3 iPos;
  attribute float iRot;
  attribute float iScale;
  attribute float iPhase;
  uniform float uTime;
  varying float vH;
  varying float vPhase;
  varying float vDist;

  vec4 SmoothTriangleWave(vec4 x) {
    vec4 t = abs(fract(x + 0.5) * 2.0 - 1.0);
    return t * t * (3.0 - 2.0 * t);
  }

  void main() {
    vH = uv.y;
    vPhase = iPhase;
    float c = cos(iRot), s = sin(iRot);
    vec3 p = position * iScale;
    p = vec3(p.x * c - p.z * s, p.y, p.x * s + p.z * c);

    // wind: sum of incommensurate triangle waves, weighted to the tip
    float w = vH * vH;
    vec2 inp = vec2(uTime * 0.9) + vec2(iPhase * 6.28);
    vec4 waves = SmoothTriangleWave(inp.xyxy * vec4(1.975, 0.793, 0.375, 0.193));
    vec2 sway = vec2(waves.x + waves.y, waves.z + waves.w) - 1.4;
    p.xz += sway * w * 0.22 * iScale;

    vec3 world = p + iPos;
    vec4 mv = viewMatrix * vec4(world, 1.0);
    vDist = -mv.z;
    gl_Position = projectionMatrix * mv;
  }
`;

const bladeFrag = /* glsl */ `
  varying float vH;
  varying float vPhase;
  varying float vDist;
  ${FOG_GLSL}

  void main() {
    vec3 root = vec3(0.016, 0.055, 0.026);
    vec3 tip = vec3(0.16, 0.46, 0.20);
    vec3 col = mix(root, tip, vH * vH);
    col *= 0.75 + 0.5 * fract(vPhase * 13.7);   // per-blade variation
    col = applyFog(col, vDist);
    gl_FragColor = vec4(col, 1.0);
  }
`;

export class Moss {
  constructor(scene) {
    const rand = mulberry32(4242);
    const COUNT = 3200;
    const Y = -6;

    // single blade: 3-segment tapered strip with a forward bend
    const base = new THREE.BufferGeometry();
    const segs = 3;
    const verts = [], uvs = [], idx = [];
    for (let i = 0; i <= segs; i++) {
      const h = i / segs;
      const wdt = 0.055 * (1 - h * 0.9);
      const bend = h * h * 0.25;
      verts.push(-wdt, h, bend, wdt, h, bend);
      uvs.push(0, h, 1, h);
    }
    for (let i = 0; i < segs; i++) {
      const o = i * 2;
      idx.push(o, o + 1, o + 2, o + 1, o + 3, o + 2);
    }
    base.setAttribute("position", new THREE.BufferAttribute(new Float32Array(verts), 3));
    base.setAttribute("uv", new THREE.BufferAttribute(new Float32Array(uvs), 2));
    base.setIndex(idx);

    const geo = new THREE.InstancedBufferGeometry();
    geo.index = base.index;
    geo.attributes.position = base.attributes.position;
    geo.attributes.uv = base.attributes.uv;

    const iPos = new Float32Array(COUNT * 3);
    const iRot = new Float32Array(COUNT);
    const iScale = new Float32Array(COUNT);
    const iPhase = new Float32Array(COUNT);
    for (let i = 0; i < COUNT; i++) {
      // ring band under the network, denser toward the far clearing
      const a = rand() * Math.PI * 2;
      const rr = 4 + 34 * Math.sqrt(rand());
      const x = Math.cos(a) * rr;
      const z = -8 + Math.sin(a) * rr * 0.9;
      iPos.set([x, Y + (rand() - 0.5) * 0.5, z], i * 3);
      iRot[i] = rand() * Math.PI * 2;
      iScale[i] = 0.7 + rand() * 1.6;
      iPhase[i] = rand();
    }
    geo.setAttribute("iPos", new THREE.InstancedBufferAttribute(iPos, 3));
    geo.setAttribute("iRot", new THREE.InstancedBufferAttribute(iRot, 1));
    geo.setAttribute("iScale", new THREE.InstancedBufferAttribute(iScale, 1));
    geo.setAttribute("iPhase", new THREE.InstancedBufferAttribute(iPhase, 1));
    geo.instanceCount = COUNT;

    this.mat = new THREE.ShaderMaterial({
      vertexShader: bladeVert,
      fragmentShader: bladeFrag,
      uniforms: { uTime: { value: 0 } },
      side: THREE.DoubleSide,
    });

    const mesh = new THREE.Mesh(geo, this.mat);
    mesh.frustumCulled = false;
    scene.add(mesh);

    // ground disc fading to fog
    const groundMat = new THREE.ShaderMaterial({
      vertexShader: /* glsl */ `
        varying vec2 vUv;
        varying float vDist;
        void main() {
          vUv = uv;
          vec4 mv = modelViewMatrix * vec4(position, 1.0);
          vDist = -mv.z;
          gl_Position = projectionMatrix * mv;
        }
      `,
      fragmentShader: /* glsl */ `
        varying vec2 vUv;
        varying float vDist;
        ${FOG_GLSL}
        void main() {
          float r = length(vUv - 0.5) * 2.0;
          vec3 col = mix(vec3(0.028, 0.075, 0.038), vec3(0.0196, 0.051, 0.031), smoothstep(0.1, 0.9, r));
          col = applyFog(col, vDist);
          gl_FragColor = vec4(col, 1.0);
        }
      `,
    });
    const ground = new THREE.Mesh(new THREE.CircleGeometry(60, 48), groundMat);
    ground.rotation.x = -Math.PI / 2;
    ground.position.set(0, Y - 0.02, -8);
    scene.add(ground);
  }

  update() {
    this.mat.uniforms.uTime.value = Time.elapsed;
  }
}
