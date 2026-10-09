import{c as o,j as s,B as r}from"./index-D2RcMjdM.js";import{C as l}from"./chevron-right-CZrZ-JKp.js";/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const p=o("House",[["path",{d:"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8",key:"5wwlr5"}],["path",{d:"M3 10a2 2 0 0 1 .709-1.528l7-5.999a2 2 0 0 1 2.582 0l7 5.999A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",key:"1d0kgt"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const d=o("MoveUp",[["path",{d:"M8 6L12 2L16 6",key:"1yvkyx"}],["path",{d:"M12 2V22",key:"r89rzk"}]]);function x(t,n){var e,a;return((a=(e=t==null?void 0:t.descriptor)==null?void 0:e.capabilities)==null?void 0:a.includes(n))??!1}function c(t){return t?[...c(t.parent),t]:[]}function u(t){return`${t.resource_type} / ${t.resource_id}`}function y({resource:t,onNavigate:n}){const e=c(t);return s.jsxs("nav",{"aria-label":"리소스 계층 경로",className:"flex flex-wrap items-center gap-1 text-xs",children:[s.jsxs(r,{type:"button",size:"sm",variant:t?"ghost":"secondary","aria-current":t?void 0:"location",onClick:()=>n(void 0),disabled:!t,children:[s.jsx(p,{className:"h-3.5 w-3.5"}),"루트"]}),e.map((a,i)=>s.jsxs("span",{className:"inline-flex items-center gap-1",children:[s.jsx(l,{"aria-hidden":"true",className:"h-3 w-3 text-muted-foreground"}),s.jsx(r,{type:"button",size:"sm",variant:i===e.length-1?"secondary":"ghost","aria-current":i===e.length-1?"location":void 0,onClick:()=>n(a),children:u(a)})]},`${a.resource_type}:${a.resource_id}:${i}`)),(t==null?void 0:t.parent)&&s.jsxs(r,{type:"button",variant:"outline",size:"sm",className:"ml-auto",onClick:()=>n(t.parent),children:[s.jsx(d,{className:"h-3.5 w-3.5"}),"상위로"]})]})}export{y as R,x as s};
