function e(t){return new TextEncoder().encode(t).length}function o(t){const n=e(t);return n<12?"too_short":n>72?"too_long":"valid"}export{o as p};
