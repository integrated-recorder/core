import{c,u as p,j as a,B as o}from"./index-0YpLawBr.js";import{C as d}from"./chevron-right-D1KcM8qo.js";/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const u=c("House",[["path",{d:"M15 21v-8a1 1 0 0 0-1-1h-4a1 1 0 0 0-1 1v8",key:"5wwlr5"}],["path",{d:"M3 10a2 2 0 0 1 .709-1.528l7-5.999a2 2 0 0 1 2.582 0l7 5.999A2 2 0 0 1 21 10v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z",key:"1d0kgt"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const h=c("MoveUp",[["path",{d:"M8 6L12 2L16 6",key:"1yvkyx"}],["path",{d:"M12 2V22",key:"r89rzk"}]]);function f(t,e){var s,n;return((n=(s=t==null?void 0:t.descriptor)==null?void 0:s.capabilities)==null?void 0:n.includes(e))??!1}function l(t){return t?[...l(t.parent),t]:[]}function m(t){return`${t.resource_type} / ${t.resource_id}`}function v({resource:t,onNavigate:e}){const{t:s}=p(),n=l(t);return a.jsxs("nav",{"aria-label":s("adapter.detail.resourcePath"),className:"flex flex-wrap items-center gap-1 text-xs",children:[a.jsxs(o,{type:"button",size:"sm",variant:t?"ghost":"secondary","aria-current":t?void 0:"location",onClick:()=>e(void 0),disabled:!t,children:[a.jsx(u,{className:"h-3.5 w-3.5"}),s("adapter.detail.root")]}),n.map((r,i)=>a.jsxs("span",{className:"inline-flex items-center gap-1",children:[a.jsx(d,{"aria-hidden":"true",className:"h-3 w-3 text-muted-foreground"}),a.jsx(o,{type:"button",size:"sm",variant:i===n.length-1?"secondary":"ghost","aria-current":i===n.length-1?"location":void 0,onClick:()=>e(r),children:m(r)})]},`${r.resource_type}:${r.resource_id}:${i}`)),(t==null?void 0:t.parent)&&a.jsxs(o,{type:"button",variant:"outline",size:"sm",className:"ml-auto",onClick:()=>e(t.parent),children:[a.jsx(h,{className:"h-3.5 w-3.5"}),s("adapter.detail.parent")]})]})}export{v as R,f as s};
