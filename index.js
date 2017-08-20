const multihashes = require('multihashes')
const promisify = require('util').promisify
const bytewise = require('bytewise')
const path = require('path')
const fs = require('fs')
const touch = require('touch')
const randomAccessFile = require('random-access-file')
const crypto = require('crypto')

const algoMap = {sha256: 'sha2-256'}

const hasher = algo => {
  return buffer => {
    let hash = crypto.createHash(algo).update(buffer).digest()
    return multihashes.encode(hash, algoMap[algo] || algo)
  }
}

const page = (hash, index, length) => {
  return bytewise.encode([hash, index, length])
}

let scan = async feed => {
  let hash = feed.hash(Buffer.from('test'))
  let pagesize = page(hash, 0, 0, 0, 0).length
  let i = 0
  let _read = promisify(cb => feed.feed.read(i, pagesize, cb))
  let read = async () => {
    try {
      let data = await _read()
      i += pagesize
      return data
    } catch (e) {
      return null
    }
  }
  let index = new Map()
  let data = true
  let seq = 0
  while (data) {
    data = await read()
    if (data) {
      let [hash, offset, length] = bytewise.decode(data)
      index.set(multihashes.toB58String(hash), [seq, offset, length])
      seq += 1
    }
  }
  return {index, i, pagesize}
}

class FS {
  constructor (str) {
    this.filename = str
    this.ram = randomAccessFile(str)
    this.write = promisify((i, value, cb) => this.ram.write(i, value, cb))
    this.read = promisify((start, end, cb) => this.ram.read(start, end, cb))
  }
  async length () {
    // TODO: implement pagesize truncation.
    if (this._offset) return this._offset
    let stat = await this.stat()
    this._offset = stat.size
    return stat.size
  }
  async stat () {
    return promisify(cb => fs.stat(this.filename, cb))()
  }
  async append (buffer) {
    if (!this._offset) await this.length()
    let start = this._offset
    this._offset = start + buffer.length
    await this.write(start, buffer)
    return start
  }
}

const defaults = {
  algo: 'sha256',
  fs: (...args) => new FS(...args)
}

class ContentFeed {
  constructor (opts) {
    if (typeof opts === 'string') {
      opts = {directory: opts}
    }
    this.opts = Object.assign({}, defaults, opts)
    // TODO: opts validation
    if (this.opts.directory) {
      this.opts.feed = path.join(this.opts.directory, 'feed')
      this.opts.store = path.join(this.opts.directory, 'store')
    }
    touch.sync(this.opts.feed)
    touch.sync(this.opts.store)
    this.feed = this.opts.fs(this.opts.feed)
    this.store = this.opts.fs(this.opts.store)
    this.hash = opts.hasher || hasher(this.opts.algo)
    this.index = scan(this)
    this._onChanges = new Set()
  }
  async append (buffer) {
    if (!Buffer.isBuffer(buffer)) throw new Error('Must be buffer type.')
    // TODO: check if we already have it
    let offset = await this.store.append(buffer)
    let hash = this.hash(buffer)
    let index = await this.index
    let len = buffer.length
    let _page = page(hash, offset, len)
    let seqOffset = await this.feed.append(_page)
    let seq = seqOffset / _page.length
    let strHash = multihashes.toB58String(hash)
    index.index.set(strHash, [seq, offset, len])
    this.onAppend(strHash, [seq, offset, len])
    return strHash
  }
  onAppend (strHash, info) {
    for (let fn of this._onChanges) {
      fn(strHash, info)
    }
  }
  async changes (opts) {
    if (!opts) opts = {}
    opts = Object.assign({since: 0}, opts)
    let hash = this.hash(Buffer.from('test'))
    let pagesize = page(hash, 0, 0).length
    let offset = opts.since * pagesize
    await this.feed.length() // make sure offset is set.
    let _changes
    if (!opts.onChange) {
      _changes = []
    }
    while (offset < this.feed._offset) {
      let data = await this.feed.read(offset, pagesize)
      let [hash, _offset, length] = bytewise.decode(data)
      let value = {
        hash: multihashes.toB58String(hash),
        offset: _offset,
        length,
        seq: offset / pagesize
      }

      if (opts.includeData) {
        value.data = await this.store.read(_offset, length)
      }

      if (_changes) _changes.push(value)
      else opts.onChange(value)
      offset += pagesize
    }
    if (opts.live) {
      // TODO: live write events
      this._onChanges.add((hash, info) => {
        let [seq, offset, length] = info
        let value = {seq, offset, length, hash}
        opts.onChange(value)
      })
    }
    return _changes
  }
  async get (hash) {
    let index = await this.index
    if (!index.index.has(hash)) throw new Error('Not found.')
    let [, offset, len] = index.index.get(hash)
    return this.store.read(offset, len)
  }
}

module.exports = (...args) => new ContentFeed(...args)
