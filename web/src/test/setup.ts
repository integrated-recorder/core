import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach, beforeEach } from 'vitest'

// Node versions with an optional built-in localStorage can expose it as
// unavailable in jsdom. Use a per-test in-memory Storage so theme preferences
// behave like a browser without persisting test state.
function createMemoryStorage(): Storage {
  const values = new Map<string, string>()
  return {
    get length() { return values.size },
    clear: () => values.clear(),
    getItem: key => values.get(String(key)) ?? null,
    key: index => [...values.keys()][index] ?? null,
    removeItem: key => { values.delete(String(key)) },
    setItem: (key, value) => { values.set(String(key), String(value)) },
  }
}

const memoryStorage = createMemoryStorage()
Object.defineProperty(globalThis, 'localStorage', { configurable: true, value: memoryStorage })
if (typeof window !== 'undefined') {
  Object.defineProperty(window, 'localStorage', { configurable: true, value: memoryStorage })
}

beforeEach(() => localStorage.clear())

afterEach(() => cleanup())
