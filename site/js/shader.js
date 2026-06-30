// Gossip-wavefront shader: the hero's signature element.
//
// Moss propagates messages by flooding/gossip — a publish ripples outward and
// overlapping ripples from many peers interfere. This shader renders exactly
// that: several moving sources emit expanding wavefronts whose interference is
// drawn as thin isolines, with rare amber crests where waves reinforce. Colors
// are read from the active theme so it recolors on theme switch. Degrades to a
// static frame under reduced-motion and to nothing if WebGL is unavailable.

const FRAG = `
precision highp float;
uniform vec2 u_res;
uniform float u_time;
uniform vec3 u_bg, u_c1, u_c2, u_ember;

float wave(vec2 p, vec2 src, float t){
  float d = length(p - src);
  return sin(d*8.5 - t*1.15) * exp(-d*1.05);
}

void main(){
  vec2 uv = (gl_FragCoord.xy*2.0 - u_res) / min(u_res.x, u_res.y);
  float t = u_time*0.55;

  vec2 s1 = vec2(sin(t*0.50)*0.75, cos(t*0.37)*0.55);
  vec2 s2 = vec2(cos(t*0.43)*0.85, sin(t*0.61)*0.60);
  vec2 s3 = vec2(sin(t*0.31+1.0)*0.60, sin(t*0.50+2.0)*0.72);
  vec2 s4 = vec2(cos(t*0.27+3.0)*0.92, cos(t*0.41+1.0)*0.42);

  float f = wave(uv,s1,t)+wave(uv,s2,t)+wave(uv,s3,t)+wave(uv,s4,t);

  float iso = abs(fract(f*1.5) - 0.5);
  float line = smoothstep(0.07, 0.0, iso);
  float amp = clamp(f*0.5+0.5, 0.0, 1.0);

  vec3 col = u_bg;
  col = mix(col, u_c1, line*0.45);
  col = mix(col, u_c2, line*amp*0.55);
  col = mix(col, u_ember, smoothstep(1.7, 2.3, f)*0.45);

  float vig = smoothstep(1.7, 0.15, length(uv));
  col *= 0.55 + 0.45*vig;

  gl_FragColor = vec4(col, 1.0);
}`;

const VERT = `attribute vec2 p; void main(){ gl_Position = vec4(p, 0.0, 1.0); }`;

function compile(gl, type, src) {
  const s = gl.createShader(type);
  gl.shaderSource(s, src);
  gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
    console.error(gl.getShaderInfoLog(s));
    return null;
  }
  return s;
}

function rgbVar(name) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const [r, g, b] = v.split(/\s+/).map(Number);
  return [r / 255, g / 255, b / 255];
}

export function initShader(canvas) {
  const gl = canvas.getContext("webgl", { antialias: false, alpha: false });
  if (!gl) { canvas.style.display = "none"; return null; }

  const prog = gl.createProgram();
  const vs = compile(gl, gl.VERTEX_SHADER, VERT);
  const fs = compile(gl, gl.FRAGMENT_SHADER, FRAG);
  if (!vs || !fs) { canvas.style.display = "none"; return null; }
  gl.attachShader(prog, vs); gl.attachShader(prog, fs); gl.linkProgram(prog);
  gl.useProgram(prog);

  const buf = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, buf);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 3, -1, -1, 3]), gl.STATIC_DRAW);
  const loc = gl.getAttribLocation(prog, "p");
  gl.enableVertexAttribArray(loc);
  gl.vertexAttribPointer(loc, 2, gl.FLOAT, false, 0, 0);

  const U = (n) => gl.getUniformLocation(prog, n);
  const u = { res: U("u_res"), time: U("u_time"), bg: U("u_bg"), c1: U("u_c1"), c2: U("u_c2"), ember: U("u_ember") };

  function colors() {
    gl.uniform3fv(u.bg, rgbVar("--c-bg"));
    gl.uniform3fv(u.c1, rgbVar("--c-accent"));
    gl.uniform3fv(u.c2, rgbVar("--c-accent2"));
    gl.uniform3fv(u.ember, rgbVar("--c-ember"));
  }

  function resize() {
    const dpr = Math.min(window.devicePixelRatio || 1, 1.75);
    const w = canvas.clientWidth * dpr, h = canvas.clientHeight * dpr;
    if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.uniform2f(u.res, canvas.width, canvas.height);
  }
  window.addEventListener("resize", resize);
  resize();
  colors();

  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  function frame(ms) {
    resize();
    gl.uniform1f(u.time, ms * 0.001);
    gl.drawArrays(gl.TRIANGLES, 0, 3);
    if (!reduce) requestAnimationFrame(frame);
  }
  if (reduce) { gl.uniform1f(u.time, 6.0); gl.drawArrays(gl.TRIANGLES, 0, 3); }
  else requestAnimationFrame(frame);

  // Recolor when the theme changes (theme.js flips data-theme).
  new MutationObserver(colors).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
  return { colors };
}
