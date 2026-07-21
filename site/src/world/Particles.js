import * as THREE from "three";
import { Time, mulberry32 } from "../core.js";

// Drifting spores: additive points filling the air, twinkling.

const vert = /* glsl */ `
  attribute float aSeed;
  uniform float uTime;
  varying float vSeed;
  varying float vDist;

  void main() {
    vSeed = aSeed;
    vec3 p = position;
    p.y += sin(uTime * 0.15 + aSeed * 40.0) * 1.2;
    p.x += cos(uTime * 0.11 + aSeed * 27.0) * 0.9;
    vec4 mv = modelViewMatrix * vec4(p, 1.0);
    vDist = -mv.z;
    gl_PointSize = (2.2 + aSeed * 3.0) * (28.0 / max(vDist, 1.0));
    gl_Position = projectionMatrix * mv;
  }
`;

const frag = /* glsl */ `
  uniform float uTime;
  varying float vSeed;
  varying float vDist;

  void main() {
    vec2 c = gl_PointCoord - 0.5;
    float d = length(c);
    float a = smoothstep(0.5, 0.0, d);
    float tw = 0.5 + 0.5 * sin(uTime * (0.8 + vSeed * 2.0) + vSeed * 90.0);
    float fade = exp(-vDist * 0.05);
    vec3 col = mix(vec3(0.35, 0.9, 0.45), vec3(0.5, 1.0, 0.75), vSeed);
    gl_FragColor = vec4(col, 1.0) * a * tw * fade * 0.55;
  }
`;

export class Particles {
  constructor(scene) {
    const rand = mulberry32(777);
    const N = 420;
    const pos = new Float32Array(N * 3);
    const seed = new Float32Array(N);
    for (let i = 0; i < N; i++) {
      pos.set(
        [(rand() - 0.5) * 36, -5 + rand() * 16, 10 - rand() * 44],
        i * 3
      );
      seed[i] = rand();
    }
    const geo = new THREE.BufferGeometry();
    geo.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    geo.setAttribute("aSeed", new THREE.BufferAttribute(seed, 1));

    this.mat = new THREE.ShaderMaterial({
      vertexShader: vert,
      fragmentShader: frag,
      uniforms: { uTime: { value: 0 } },
      blending: THREE.AdditiveBlending,
      transparent: true,
      depthWrite: false,
    });

    const pts = new THREE.Points(geo, this.mat);
    pts.frustumCulled = false;
    scene.add(pts);
  }

  update() {
    this.mat.uniforms.uTime.value = Time.elapsed;
  }
}
