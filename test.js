'use strict';

import { assert } from 'chai';
import { InstanceStore } from './src';

describe('InstanceStore', function() {
  let store;

  async function catchError(fn) {
    let err;
    try {
      await fn();
    } catch (e) {
      err = e;
    }
    return err;
  }

  before(async function() {
    store = new InstanceStore({
      name: 'Test',
      url: 'mysql://test@localhost/test',
      classes: [
        {
          name: 'Account',
          indexes: ['accountNumber', 'country']
        },
        {
          name: 'Person',
          indexes: ['accountNumber', 'country', ['lastName', 'firstName']]
        },
        {
          name: 'Company',
          indexes: ['accountNumber', 'country', 'name']
        }
      ]
    });
  });

  after(async function() {
    await store.destroyAll();
  });

  it('should handle basic operations', async function() {
    this.timeout(60000);

    let classes = ['Account', 'Person'];
    let key = 'mvila';
    let instance = {
      accountNumber: 12345,
      firstName: 'Manuel',
      lastName: 'Vila',
      country: 'France'
    };
    await store.put(classes, key, instance);

    let result = await store.get('Account', key);
    assert.deepEqual(result.classes, classes);
    assert.deepEqual(result.instance, instance);

    result = await store.get('Person', key);
    assert.deepEqual(result.classes, classes);
    assert.deepEqual(result.instance, instance);

    let err = await catchError(async function() {
      await store.get('Company', key);
    });
    assert.instanceOf(err, Error);

    let hasBeenDeleted = await store.delete('Person', key);
    assert.isTrue(hasBeenDeleted);
    result = await store.get('Person', key, { errorIfMissing: false });
    assert.isUndefined(result);
    hasBeenDeleted = await store.delete('Person', key, { errorIfMissing: false });
    assert.isFalse(hasBeenDeleted);
  });

  describe('with several instances', function() {
    beforeEach(async function() {
      await store.put(['Account'], 'aaa', {
        accountNumber: 45329,
        country: 'France'
      });
      await store.put(['Account', 'Person'], 'bbb', {
        accountNumber: 3246,
        firstName: 'Jack',
        lastName: 'Daniel',
        country: 'USA'
      });
      await store.put(['Account', 'Company'], 'ccc', {
        accountNumber: 7002,
        name: 'Kinda Ltd',
        country: 'China'
      });
      await store.put(['Account', 'Person'], 'ddd', {
        accountNumber: 55498,
        firstName: 'Vincent',
        lastName: 'Vila',
        country: 'USA'
      });
      await store.put(['Account', 'Person'], 'eee', {
        accountNumber: 888,
        firstName: 'Pierre',
        lastName: 'Dupont',
        country: 'France'
      });
      await store.put(['Account', 'Company'], 'fff', {
        accountNumber: 8775,
        name: 'Fleur SARL',
        country: 'France'
      });
    });

    afterEach(async function() {
      await store.delete('Account', 'aaa', { errorIfMissing: false });
      await store.delete('Account', 'bbb', { errorIfMissing: false });
      await store.delete('Account', 'ccc', { errorIfMissing: false });
      await store.delete('Account', 'ddd', { errorIfMissing: false });
      await store.delete('Account', 'eee', { errorIfMissing: false });
      await store.delete('Account', 'fff', { errorIfMissing: false });
    });

    it('should be able to get many of them', async function() {
      let results = await store.getMany('Account', ['aaa', 'ccc']);
      assert.strictEqual(results.length, 2);
      assert.deepEqual(results[0].classes, ['Account']);
      assert.strictEqual(results[0].key, 'aaa');
      assert.strictEqual(results[0].instance.accountNumber, 45329);
      assert.deepEqual(results[1].classes, ['Account', 'Company']);
      assert.strictEqual(results[1].key, 'ccc');
      assert.strictEqual(results[1].instance.accountNumber, 7002);
    });

    it('should be able to find those belonging to a class', async function() {
      let results = await store.find('Company');
      assert.strictEqual(results.length, 2);
      assert.deepEqual(results[0].classes, ['Account', 'Company']);
      assert.strictEqual(results[0].key, 'ccc');
      assert.strictEqual(results[0].instance.name, 'Kinda Ltd');
      assert.deepEqual(results[1].classes, ['Account', 'Company']);
      assert.strictEqual(results[1].key, 'fff');
      assert.strictEqual(results[1].instance.name, 'Fleur SARL');
    });

    it('should be able to find and order', async function() {
      let results = await store.find('Person', { order: 'accountNumber' });
      assert.strictEqual(results.length, 3);
      let numbers = results.map(result => result.instance.accountNumber);
      assert.deepEqual(numbers, [888, 3246, 55498]);
    });

    it('should be able to find with a query', async function() {
      let results = await store.find('Account', {
        query: { country: 'USA' }
      });
      let keys = results.map(result => result.key);
      assert.deepEqual(keys, ['bbb', 'ddd']);

      results = await store.find('Company', {
        query: { country: 'UK' }
      });
      assert.strictEqual(results.length, 0);
    });

    it('should be able to count those belonging to a class', async function() {
      let count = await store.count('Person');
      assert.strictEqual(count, 3);
    });

    it('should be able to count with a query', async function() {
      let count = await store.count('Account', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 3);

      count = await store.count('Person', {
        query: { country: 'France' }
      });
      assert.strictEqual(count, 1);

      count = await store.count('Company', {
        query: { country: 'Spain' }
      });
      assert.strictEqual(count, 0);
    });

    it('should be able to iterate found results', async function() {
      let keys = [];
      await store.forEach('Account', { batchSize: 2 }, async function(result) {
        keys.push(result.key);
      });
      assert.deepEqual(keys, ['aaa', 'bbb', 'ccc', 'ddd', 'eee', 'fff']);
    });

    it('should be able to find and delete', async function() {
      let options = { query: { country: 'France' }, batchSize: 2 };
      let deletedInstancesCount = await store.findAndDelete('Account', options);
      assert.strictEqual(deletedInstancesCount, 3);
      let results = await store.find('Account');
      let keys = results.map(result => result.key);
      assert.deepEqual(keys, ['bbb', 'ccc', 'ddd']);
      deletedInstancesCount = await store.findAndDelete('Account', options);
      assert.strictEqual(deletedInstancesCount, 0);
    });

    it('should be able to change an instance inside a transaction', async function() {
      assert.isFalse(store.insideTransaction);
      await store.transaction(async function(transaction) {
        assert.isTrue(transaction.insideTransaction);
        let innerResult = await transaction.get('Person', 'bbb');
        assert.strictEqual(innerResult.instance.lastName, 'Daniel');
        innerResult.instance.lastName = 'D.';
        await transaction.put(['Account', 'Person'], 'bbb', innerResult.instance);
        innerResult = await transaction.get('Person', 'bbb');
        assert.strictEqual(innerResult.instance.lastName, 'D.');
      });
      let result = await store.get('Person', 'bbb');
      assert.strictEqual(result.instance.lastName, 'D.');
    });

    it('should be able to rollback a change inside an aborted transaction', async function() {
      let err = await catchError(async function() {
        assert.isFalse(store.insideTransaction);
        await store.transaction(async function(transaction) {
          assert.isTrue(transaction.insideTransaction);
          let innerResult = await transaction.get('Person', 'bbb');
          assert.strictEqual(innerResult.instance.lastName, 'Daniel');
          innerResult.instance.lastName = 'D.';
          await transaction.put(['Account', 'Person'], 'bbb', innerResult.instance);
          innerResult = await transaction.get('Person', 'bbb');
          assert.strictEqual(innerResult.instance.lastName, 'D.');
          throw new Error('Something wrong');
        });
      });
      assert.instanceOf(err, Error);
      let result = await store.get('Person', 'bbb');
      assert.strictEqual(result.instance.lastName, 'Daniel');
    });
  }); // with several items
});
