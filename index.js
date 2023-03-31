//jshint esversion: 6
//jshint -W033
const pull = require('pull-stream')
const pullAbortable = require('pull-abortable')
const Looper = require('pull-looper')
const wrap = require('./wrap')
const debug = require('debug')('flumedb-view-driver')
const explain = require('explain-error')

module.exports = function (flume, log, stream) {
  return use

  function buildView(sv) {
    sv.since.once(function build (upto) {
      debug(`${sv.name} is at ${upto}`)
      const rebuildView = () => {
        debug(`rebuilding ${sv.name}`)
        // TODO - create some debug logfile and log that we're rebuilding, what error was
        // so that we have some visibility on how often this happens over time
        sv.setDestroying(true)
        sv.destroy(() => {
          sv.setDestroying(false)
          build(-1)
        })
      }

      log.since.once(function (since) {
        if (upto > since) rebuildView()
        else {
          var opts = { gt: upto, live: true, seqs: true, values: true }
          if (upto === -1) opts.cache = false

          // Before starting a stream it's important that we configure some way
          // for FlumeDB to close the stream! Previous versions of FlumeDB
          // would rely on each view having some intelligent stream closure
          // that seemed to cause lots of race conditions. My hope is that if
          // we manage strema cancellations in FlumeDB rather than each
          // individual Flumeview then we'll have fewer race conditions.
          const abortable = pullAbortable()
          sv.abortStream = abortable.abort

          pull(
            stream(sv, opts),
            abortable,
            Looper,
            sv.createSink(function (err) {
              // If FlumeDB is closed or this view is currently being
              // destroyed, we don't want to try to immediately restart the
              // stream. This saves us a bit of sanity
              if (!flume.closed && sv.isDestroying() === false) {
                // The `err` value is meant to be either `null` or an error,
                // but unfortunately Pull-Write seems to abort streams with
                // an error when it shouldn't. Fortunately these errors have
                // an extra `{ abort. true }` property, which makes them easy
                // to identify. Errors where `{ abort: true }` should be
                // handled as if the error was `null`.
                if (err && err.abort !== true) {
                  console.error(
                    explain(err, `rebuilding ${sv.name} after view stream error`)
                  )
                  rebuildView()
                } else {
                  sv.since.once(build)
                }
              }
            })
          )
        }
      })
    })
  }

  function use(name, createView) {
    const sv = createView({
      get: flume.get,
      stream: flume.stream,
      since: log.since,
      filename: log.filename 
    }, name)

    const requiredMethods = ['close', 'createSink', 'destroy', 'since']

    requiredMethods.forEach((methodName) => {
      if (typeof sv[methodName] !== 'function') {
        throw new Error(
          `FlumeDB view '${name}' must implement method '${methodName}'`
        )
      }
    })

    const wrapped = wrap(sv, flume)
    wrapped.name = name

    // if the view is ahead of the log somehow, destroy the view and rebuild it.
    buildView(wrapped)
    
    return wrapped
  }

}
