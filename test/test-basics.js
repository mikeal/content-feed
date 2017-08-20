const createFeed = require('../')
const test = require('tap').test
const path = require('path')
const mkdirp = require('mkdirp')
const os = require('os')
const touch = require('touch')

const tmpdir = () => {
  let dir = path.join(os.tmpdir(), Math.random().toString())
  mkdirp.sync(dir)
  return dir
}

test('basic append', async t => {
  t.plan(2)
  let feed = createFeed(tmpdir())
  let hash = await feed.append(Buffer.from('test'))
  t.same(await feed.get(hash), Buffer.from('test'))
  hash = await feed.append(Buffer.from('asdf'))
  t.same(await feed.get(hash), Buffer.from('asdf'))
})

test('load from directory', async t => {
  t.plan(2)
  let dir = tmpdir()
  let feed = createFeed(dir)
  let hash1 = await feed.append(Buffer.from('test'))
  let hash2 = await feed.append(Buffer.from('asdf'))
  let feed2 = createFeed(dir)
  t.same(await feed2.get(hash1), Buffer.from('test'))
  t.same(await feed2.get(hash2), Buffer.from('asdf'))
})

test('changes static', async t => {
  t.plan(2)
  let feed = createFeed(tmpdir())
  await feed.append(Buffer.from('test'))
  await feed.append(Buffer.from('asdf'))
  let changes = await feed.changes()
  let expected = [
    { hash: 'QmZ5NmGeStdit7tV6gdak1F8FyZhPsfA843YS9f2ywKH6w',
      offset: 0,
      length: 4,
      seq: 0 },
    { hash: 'QmeYzshSoNHr2QUWqmkMAy6raRhcmzTuroy7johWJNn3fY',
      offset: 4,
      length: 4,
      seq: 1 }
  ]
  t.same(expected, changes)
  let changes2 = await feed.changes({includeData: true})
  expected[0].data = Buffer.from('test')
  expected[1].data = Buffer.from('asdf')
  t.same(expected, changes2)
})

test('changes onChange', async t => {
  t.plan(2)
  let feed = createFeed(tmpdir())
  await feed.append(Buffer.from('test'))

  let expected = [
    { hash: 'QmZ5NmGeStdit7tV6gdak1F8FyZhPsfA843YS9f2ywKH6w',
      offset: 0,
      length: 4,
      seq: 0 },
    { hash: 'QmeYzshSoNHr2QUWqmkMAy6raRhcmzTuroy7johWJNn3fY',
      offset: 4,
      length: 4,
      seq: 1 }
  ]

  let onChange = change => {
    t.same(change, expected.shift())
  }
  await feed.changes({live: true, onChange})
  await feed.append(Buffer.from('asdf'))
  await feed.changes()
})

test('basic append w/ sha1', async t => {
  t.plan(4)
  let feed = createFeed({directory: tmpdir(), algo: 'sha1'})
  let hash = await feed.append(Buffer.from('test'))
  t.same(hash, '5dt9CqvXK9qs7vazf7k7ZRqe28VPTg')
  t.same(await feed.get(hash), Buffer.from('test'))
  hash = await feed.append(Buffer.from('asdf'))
  t.same(hash, '5dreDuCVa7sH2pDn3tsbxeyhw5BXiB')
  t.same(await feed.get(hash), Buffer.from('asdf'))
})

test('not found', async t => {
  t.plan(2)
  let feed = createFeed(tmpdir())
  try {
    await feed.get('notfound')
  } catch (e) {
    t.type(e, 'Error')
    t.same(e.message, 'Not found.')
  }
})

test('append non-buffer', async t => {
  t.plan(2)
  let feed = createFeed(tmpdir())
  try {
    await feed.append('notfound')
  } catch (e) {
    t.type(e, 'Error')
    t.same(e.message, 'Must be buffer type.')
  }
})

test('pass store and feed files', async t => {
  t.plan(1)
  let dir = tmpdir()
  let feed = path.join(dir, 'feed.test')
  let store = path.join(dir, 'store.test')
  touch.sync(feed)
  touch.sync(store)
  let _feed = createFeed({feed, store})
  let hash = await _feed.append(Buffer.from('test'))
  t.same(await _feed.get(hash), Buffer.from('test'))
})
