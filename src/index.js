'use strict';

import { clone, cloneDeep } from 'better-clone';
import setImmediatePromise from 'set-immediate-promise';
import { EventEmitterMixin } from 'event-emitter-mixin';
import DocumentStore from 'document-store';

const VERSION = 2;
const COLLECTION_NAME = 'Objects';
const RESPIRATION_RATE = 250;

export class InstanceStore extends EventEmitterMixin() {
  constructor(options = {}) {
    super();

    if (!options.name) throw new Error('Instance store name is missing');
    if (!options.url) throw new Error('Instance store URL is missing');

    if (options.log) this.log = options.log;

    this.name = options.name;

    let collection = {
      name: COLLECTION_NAME,
      indexes: []
    };

    (options.classes || []).forEach(klass => {
      if (typeof klass === 'string') klass = { name: klass };
      let name = klass.name;
      function fn(item) {
        return item._classes && item._classes.indexOf(name) !== -1 ? true : undefined;
      }
      fn.displayName = this.makeIndexName(name);
      let indexes = cloneDeep(klass.indexes) || [];
      indexes.unshift([]); // Trick to add an index for the class itself
      indexes.forEach(index => {
        if (typeof index === 'string' || typeof index === 'function' || Array.isArray(index)) index = { properties: index };
        let properties = index.properties;
        if (!Array.isArray(properties)) properties = [properties];
        properties.unshift(fn);
        index.properties = properties;
        if (index.projection) index.projection.push('_classes');
        collection.indexes.push(index);
      });
    });

    this.documentStore = new DocumentStore({
      name: options.name,
      url: options.url,
      collections: [collection],
      log: options.log
    });

    this.documentStore.on('willUpgrade', () => this.emit('willUpgrade'));
    this.documentStore.on('didUpgrade', () => this.emit('didUpgrade'));
    this.documentStore.on('willMigrate', () => this.emit('willMigrate'));
    this.documentStore.on('didMigrate', () => this.emit('didMigrate'));

    this.root = this;
  }

  get store() {
    return this.documentStore.store;
  }

  // === Database ====

  async initializeInstanceStore() {
    if (this.hasBeenInitialized) return;
    if (this.isInitializing) return;
    if (this.insideTransaction) {
      throw new Error('Cannot initialize the instance store inside a transaction');
    }
    this.isInitializing = true;
    try {
      await this.documentStore.initializeDocumentStore();
      let hasBeenCreated = await this.createInstanceStoreIfDoesNotExist();
      if (!hasBeenCreated) {
        await this.documentStore.lockDocumentStore();
        try {
          await this.upgradeInstanceStore();
        } finally {
          await this.documentStore.unlockDocumentStore();
        }
      }
      this.hasBeenInitialized = true;
      await this.emit('didInitialize');
    } finally {
      this.isInitializing = false;
    }
  }

  async _loadInstanceStoreRecord(storeTransaction = this.store, errorIfMissing = true) {
    return await storeTransaction.get(
      [this.name, '$InstanceStore'],
      { errorIfMissing }
    );
  }

  async _saveInstanceStoreRecord(record, storeTransaction = this.store, errorIfExists) {
    await storeTransaction.put([this.name, '$InstanceStore'], record, {
      errorIfExists,
      createIfMissing: !errorIfExists
    });
  }

  async createInstanceStoreIfDoesNotExist() {
    let hasBeenCreated = false;
    await this.store.transaction(async function(storeTransaction) {
      let record = await this._loadInstanceStoreRecord(storeTransaction, false);
      if (!record) {
        record = {
          name: this.name,
          version: VERSION
        };
        await this._saveInstanceStoreRecord(record, storeTransaction, true);
        hasBeenCreated = true;
        await this.emit('didCreate');
        if (this.log) {
          this.log.info(`Instance store '${this.name}' created`);
        }
      }
    }.bind(this));
    return hasBeenCreated;
  }

  async upgradeInstanceStore() {
    let record = await this._loadInstanceStoreRecord();
    let version = record.version;

    if (version === VERSION) return;

    if (version > VERSION) {
      throw new Error('Cannot downgrade the instance store');
    }

    this.emit('willUpgrade');

    if (version < 2) {
      throw new Error('Cannot upgrade the instance store to version 2');
    }

    record.version = VERSION;
    await this._saveInstanceStoreRecord(record);
    if (this.log) {
      this.log.info(`Instance store '${this.name}' upgraded to version ${VERSION}`);
    }

    this.emit('didUpgrade');
  }

  async destroyAll() {
    if (this.insideTransaction) {
      throw new Error('Cannot destroy an instance store inside a transaction');
    }
    await this.documentStore.destroyAll();
    this.hasBeenInitialized = false;
  }

  async close() {
    await this.documentStore.close();
  }

  // === Basic operations ====

  // Options:
  //   errorIfMissing: throw an error if the instance is not found. Default: true.
  async get(klass, key, options) {
    this.checkClass(klass);
    await this.initializeInstanceStore();
    let document = await this.documentStore.get(COLLECTION_NAME, key, options);
    if (!document) return undefined; // means instance is not found and errorIfMissing is false
    let classes = document._classes;
    if (classes.indexOf(klass) === -1) {
      throw new Error('Found an instance with the specified key but not belonging to the specified class');
    }
    let instance = document;
    delete instance._classes;
    return { classes, key, instance };
  }

  // Options:
  //   createIfMissing: add the instance if it is missing.
  //     If the instance is already present, replace it. Default: true.
  //   errorIfExists: throw an error if the instance is already present.
  //     Default: false.
  async put(classes, key, instance, options) {
    if (!Array.isArray(classes)) throw new Error('classes parameter is invalid');
    if (!classes.length) throw new Error('classes parameter is empty');
    let document = clone(instance);
    document._classes = classes;
    await this.initializeInstanceStore();
    await this.documentStore.put(COLLECTION_NAME, key, document, options);
  }

  // Options:
  //   errorIfMissing: throw an error if the instance is not found. Default: true.
  async delete(klass, key, options) {
    this.checkClass(klass);
    let hasBeenDeleted = false;
    await this.transaction(async function(transaction) {
      let document = await transaction.documentStore.get(COLLECTION_NAME, key, options);
      if (!document) return; // means instance not found and errorIfMissing false
      if (document._classes.indexOf(klass) === -1) {
        throw new Error('Found an instance with the specified key but not belonging to the specified class');
      }
      hasBeenDeleted = await transaction.documentStore.delete(COLLECTION_NAME, key);
    });
    return hasBeenDeleted;
  }

  async getMany(klass, keys, options) {
    this.checkClass(klass);
    let iterationsCount = 0;
    await this.initializeInstanceStore();
    let results = await this.documentStore.getMany(COLLECTION_NAME, keys, options);
    let finalResults = [];
    for (let result of results) {
      let classes = result.document._classes;
      if (classes.indexOf(klass) === -1) {
        throw new Error('Found an instance with the specified key but not belonging to the specified class');
      }
      let key = result.key;
      let instance = result.document;
      delete instance._classes;
      finalResults.push({ classes, key, instance });
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return finalResults;
  }

  // Options:
  //   query: specifies the search query.
  //     Example: { blogId: 'xyz123', postId: 'abc987' }.
  //   order: specifies the property to order the results by:
  //     Example: ['lastName', 'firstName'].
  //   start, startAfter, end, endBefore: ...
  //   reverse: if true, the search is made in reverse order.
  //   properties: indicates properties to fetch. '*' for all properties
  //     or an array of property name. If an index projection matches
  //     the requested properties, the projection is used.
  //   limit: maximum number of instances to return.
  async find(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    let iterationsCount = 0;
    await this.initializeInstanceStore();
    let results = await this.documentStore.find(COLLECTION_NAME, options);
    let finalResults = [];
    for (let result of results) {
      let classes = result.document._classes;
      let key = result.key;
      let instance = result.document;
      delete instance._classes;
      finalResults.push({ classes, key, instance });
      if (++iterationsCount % RESPIRATION_RATE === 0) await setImmediatePromise();
    }
    return finalResults;
  }

  // Options: same as find() without 'reverse' and 'properties' attributes.
  async count(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    await this.initializeInstanceStore();
    return await this.documentStore.count(COLLECTION_NAME, options);
  }

  // === Composed operations ===

  // Options: same as find() plus:
  //   batchSize: use several find() operations with batchSize as limit.
  //     Default: 250.
  async forEach(klass, options, fn, thisArg) {
    options = this.injectClassInQueryOption(klass, options);
    await this.initializeInstanceStore();
    await this.documentStore.forEach(COLLECTION_NAME, options, async function(document, key) {
      let classes = document._classes;
      let instance = document;
      delete instance._classes;
      await fn.call(thisArg, { classes, key, instance });
    });
  }

  // Options: same as forEach() without 'properties' attribute.
  async findAndDelete(klass, options) {
    options = this.injectClassInQueryOption(klass, options);
    await this.initializeInstanceStore();
    return await this.documentStore.findAndDelete(COLLECTION_NAME, options);
  }

  // === Transactions ====

  async transaction(fn) {
    if (this.insideTransaction) return await fn(this);
    await this.initializeInstanceStore();
    return await this.documentStore.transaction(async function(documentStoreTransaction) {
      let transaction = Object.create(this);
      transaction.documentStore = documentStoreTransaction;
      return await fn(transaction);
    }.bind(this));
  }

  get insideTransaction() {
    return this !== this.root;
  }

  // === Helpers ====

  checkClass(klass) {
    if (typeof klass !== 'string') throw new Error('class parameter is invalid');
    if (!klass) throw new Error('class parameter is missing or empty');
  }

  injectClassInQueryOption(klass, options = {}) {
    this.checkClass(klass);
    if (!options.query) options.query = {};
    options.query[this.makeIndexName(klass)] = true;
    return options;
  }

  makeIndexName(klass) {
    return klass + '?';
  }
}

export default InstanceStore;
