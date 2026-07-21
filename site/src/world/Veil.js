import * as THREE from "three";
import { Time } from "../core.js";

// Full-screen noisy iris rendered ON TOP of the frame (autoClear=false trick).
// uProgress 0 = fully covered, 1 = fully revealed.

const frag = /* glsl */ `
  uniform float uProgress;
  uniform float uTime;
  uniform vec2 uRes;
  varying vec2 vUv;

  float hash(vec2 p) { return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453); }
  float noise(vec2 p) {
    vec2 i = floor(p), f = fract(p);
    f = f * f * (3.0 - 2.0 * f);
    return mix(mix(hash(i), hash(i + vec2(1, 0)), f.x),
               mix(hash(i + vec2(0, 1)), hash(i + vec2(1, 1)), f.x), f.y);
  }
  float fbm(vec2 p) {
    float v = 0.0, a = 0.5;
    for (int i = 0; i < 4; i++) { v += a * noise(p); p *= 2.1; a *= 0.5; }
    return v;
  }

  void main() {
    vec2 uv = vUv;
    vec2 c = uv - 0.5;
    c.x *= uRes.x / uRes.y;
    float n = fbm(uv * 5.0 + uTime * 0.08);
    float r = length(c) + (n * n) * 0.55;
    // expanding hole: covered outside the circle, open inside; at uProgress 1
    // the radius exceeds any pixel (incl. fbm offset)
    float edge = uProgress * 2.3;
    float m = smoothstep(edge - 0.18, edge, r);   // 1 = covered
    vec3 col = mix(vec3(0.02, 0.051, 0.031), vec3(0.10, 0.30, 0.14), n * 0.6);
    gl_FragColor = vec4(col, m);
    if (m < 0.003) discard;
  }
`;

export class Veil {
  constructor() {
    this.scene = new THREE.Scene();
    this.camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0, 1);
    this.progress = 0;
    this.done = false;

    this.mat = new THREE.ShaderMaterial({
      vertexShader: /* glsl */ `
        varying vec2 vUv;
        void main() { vUv = uv; gl_Position = vec4(position.xy, 0.0, 1.0); }
      `,
      fragmentShader: frag,
      uniforms: {
        uProgress: { value: 0 },
        uTime: { value: 0 },
        uRes: { value: new THREE.Vector2(1, 1) },
      },
      transparent: true,
      depthTest: false,
      depthWrite: false,
    });
    this.scene.add(new THREE.Mesh(new THREE.PlaneGeometry(2, 2), this.mat));
  }

  render(renderer, w, h) {
    if (this.done) return;
    this.mat.uniforms.uProgress.value = this.progress;
    this.mat.uniforms.uTime.value = Time.elapsed;
    this.mat.uniforms.uRes.value.set(w, h);
    renderer.render(this.scene, this.camera);
    if (this.progress >= 1) this.done = true;
  }
}
