var utils = require('../utils/utils')
// var formatters = require('./formatters');
var sha3 = require('../utils/sha3')
var coder = require('web3/lib/solidity/coder')

var config = require('../utils/config')
var ZERO_ADDRESS = Buffer.from('0000000000000000000000000000000000000000', 'hex')

var functionSig = function (abi) {
  var name = utils.transformToFullName(abi)
  return sha3(name).slice(0, 8)
}

var types = function (args) {
  return args.map(function (arg) {
    return arg.type
  })
}

var txPayload = function (abi, args, account, address, bytecode) {
  var payload = {}
  // Data packing is different if calling or creating
  var data
  if (!address) {
    data = bytecode
    if (abi) data += coder.encodeParams(types(abi.inputs), args)
  } else {
    data = functionSig(abi) + coder.encodeParams(types(abi.inputs), args)
  }

  payload.Input = {Address: Buffer.from(account || ZERO_ADDRESS, 'hex'), Amount: 1}
  payload.Address = address ? Buffer.from(address, 'hex') : ''
  payload.GasLimit = config.DEFAULT_GAS
  payload.Fee = config.DEFAULT_FEE
  payload.Data = Buffer.from(data, 'hex')

  return payload
}

var unpackOutput = function (output, abi, objectReturn) {
  if (!output) {
    return
  }

  var outputs = abi.outputs
  var outputTypes = types(outputs)

  // Decode raw bytes to arguments
  var raw = coder.decodeParams(outputTypes, output.toString('hex').toUpperCase())

  for (var i = 0; i < outputTypes.length; i++) {
    if (/int/i.test(outputTypes[i])) {
      raw[i] = raw[i].toNumber()
    }
  };

  if (!objectReturn) {
    return raw
  }

  // If an object is wanted,
  var result = {raw: raw.slice()}

  var args = outputs.reduce(function (acc, current) {
    var value = raw.shift()
    if (current.name) {
      acc[current.name] = value
    }
    return acc
  }, {})

  result.values = args

  return result
}

/**
 * Calls a contract function.
 *
 * @method call
 * @param {...Object} Contract function arguments
 * @param {function}
 * @return {String} output bytes
 */
var SolidityFunction = function (abi) {
  var isCon = (abi == null || abi.type === 'constructor')
  var name
  var displayName
  var typeName

  if (!isCon) {
    name = utils.transformToFullName(abi)
    displayName = utils.extractDisplayName(name)
    typeName = utils.extractTypeName(name)
  }

  var call = function () {
    var args = Array.prototype.slice.call(arguments)
    var isSim = args.shift()
    var address = args.shift() || this.address

    var callback
    if (utils.isFunction(args[args.length - 1])) { callback = args.pop() };

    var self = this

    var P = new Promise(function (resolve, reject) {
      if (address == null && !isCon) reject(new Error('Address not provided to call'))
      if (abi != null && abi.inputs.length !== args.length) reject(new Error('Insufficient arguments passed to function: ' + (isCon ? 'constructor' : name)))
      // Post process the return
      var post = function (error, result) {
        if (error) return reject(error)

        if (result.Exception && result.Exception.Code === 16) {
          // Execution was reverted
          // Strip first 4 bytes(function signature) the decode as a string
          var errorString = coder.decodeParams(['string'], result.Result.Return.slice(4).toString('hex').toUpperCase())[0]
          return reject(errorString)
        }
        
        if (isCon) return resolve(result.Receipt.ContractAddress.toString('hex').toUpperCase())

        var unpacked = null
        try {
          unpacked = unpackOutput(result.Result.Return, abi, self.objectReturn)
        } catch (e) {
          return reject(e)
        }
        return resolve(utils.web3ToBurrow(unpacked))
      }

      // Decide if to make a "call" or a "transaction"
      // For the moment we need to use the burrowtoweb3 function to prefix bytes with 0x
      // otherwise the coder will give an error with bugnumber not a number
      // TODO investigate if other libs or an updated lib will fix this

      var payload = txPayload(abi, utils.burrowToWeb3(args), self.burrow.account || ZERO_ADDRESS, address, self.code)

      if (isSim) {
        self.burrow.pipe.call(payload, post)
      } else {
        self.burrow.pipe.transact(payload, post)
      }
    })

    if (callback) {
      P.then((result) => { return callback(null, result) })
        .catch((err) => { return callback(err) })
    } else {
      return P
    }
  }

  return {displayName, typeName, call}
}

module.exports = SolidityFunction
