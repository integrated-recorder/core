import{c as s,j as c,t as o,aj as i,R as p,ak as g}from"./index-DxLws8kj.js";import{T as r,L as n}from"./triangle-alert-My0cBVo0.js";/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const a=s("CircleDashed",[["path",{d:"M10.1 2.182a10 10 0 0 1 3.8 0",key:"5ilxe3"}],["path",{d:"M13.9 21.818a10 10 0 0 1-3.8 0",key:"11zvb9"}],["path",{d:"M17.609 3.721a10 10 0 0 1 2.69 2.7",key:"1iw5b2"}],["path",{d:"M2.182 13.9a10 10 0 0 1 0-3.8",key:"c0bmvh"}],["path",{d:"M20.279 17.609a10 10 0 0 1-2.7 2.69",key:"1ruxm7"}],["path",{d:"M21.818 10.1a10 10 0 0 1 0 3.8",key:"qkgqxc"}],["path",{d:"M3.721 6.391a10 10 0 0 1 2.7-2.69",key:"1mcia2"}],["path",{d:"M6.391 20.279a10 10 0 0 1-2.69-2.7",key:"1fvljs"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const u=s("CircleX",[["circle",{cx:"12",cy:"12",r:"10",key:"1mglay"}],["path",{d:"m15 9-6 6",key:"1uzhvr"}],["path",{d:"m9 9 6 6",key:"z0biqf"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const d=s("Square",[["rect",{width:"18",height:"18",x:"3",y:"3",rx:"2",key:"afitv7"}]]),k={recording:"green",stopped:"neutral",completed:"blue",interrupted:"amber",failed:"red",verified:"green",degraded:"amber",unknown:"neutral",verifying:"blue",queued:"neutral",running:"blue",ready:"green",unavailable:"amber",rejected:"red",disabled:"neutral",offline:"neutral",checking:"blue",starting:"blue",backoff:"amber",attention_required:"red",suppressed:"neutral"},h={recording:p,stopped:d,completed:i,interrupted:r,failed:u,verified:i,degraded:r,verifying:n,unknown:a,queued:a,running:n,ready:i,unavailable:r,rejected:u,disabled:d,offline:a,checking:n,starting:n,backoff:a,attention_required:r,suppressed:d};function b({state:t}){const e=(t==null?void 0:t.toLowerCase())??"unknown",l=h[e]??a;return c.jsxs(o,{tone:k[e]??"neutral",children:[c.jsx(l,{className:`h-3 w-3 shrink-0 ${e==="running"||e==="verifying"||e==="restarting"?"animate-spin":""}`}),g(e)]})}export{u as C,b as S};
