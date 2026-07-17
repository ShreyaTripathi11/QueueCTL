'use strict';

// delay = base ^ attempts (seconds)
function computeDelaySeconds(base, attempts) {
  return Math.pow(base, attempts);
}

module.exports = { computeDelaySeconds };
