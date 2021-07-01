package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determin the number of primes <= limit.
 */
public final class SieveActor extends Sieve {
    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {

        SieveActorActor sieveActorActor = new SieveActorActor(2);
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                sieveActorActor.send(i);
            }
        });

        int numPrimes = 0;
        SieveActorActor loopActor = sieveActorActor;
        while (loopActor != null) {
            numPrimes += loopActor.numLocalPrimes;
            loopActor = loopActor.nextActor;
        }
        return numPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private static final int MAX_LOCAL_PRIMES = 500;
        private final int[] localPrimes;
        private int numLocalPrimes;
        private SieveActorActor nextActor;

        SieveActorActor(final int localPrime) {
            this.localPrimes = new int[MAX_LOCAL_PRIMES];
            this.localPrimes[0] = localPrime;
            this.numLocalPrimes = 1;
            this.nextActor = null;
        }

        /**
         * Process a single message sent to this actor.
         * <p>
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int candidate = (Integer) msg;

            final boolean locallyPrime = isLocallyPrime(candidate);
            if (locallyPrime) {
                if (numLocalPrimes < MAX_LOCAL_PRIMES) {
                    localPrimes[numLocalPrimes] = candidate;
                    numLocalPrimes += 1;
                } else if (nextActor == null) {
                    nextActor = new SieveActorActor(candidate);
                } else {
                    nextActor.send(msg);
                }
            }

        }

        private boolean isLocallyPrime(final int candidate) {
            final boolean[] isPrime = {true};
            checkPrimeKernel(candidate, isPrime, 0, numLocalPrimes);
            return isPrime[0];
        }

        private void checkPrimeKernel(int candidate, boolean[] isPrime, int startIndex, int endIndex) {
            for (int i = startIndex; i < endIndex; i++) {
                if (candidate % localPrimes[i] == 0) {
                    isPrime[0] = false;
                    break;
                }
            }
        }
    }
}
