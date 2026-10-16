//! Cadence clock limiter and x/time/rate single-token arithmetic.
//! Callers synchronize this state and supply Unix-nanosecond clock samples.
//
// Copyright 2009 The Go Authors.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

const ZERO_TIME: i128 = -62_135_596_800_000_000_000;
const SECOND: i64 = 1_000_000_000;
#[derive(Clone, Debug)]
pub struct Reservation {
    ok: bool,
    tokens: i64,
    at: i128,
    reserved_at: i128,
    rate: f64,
}
impl Reservation {
    pub fn allowed(&self) -> bool {
        self.ok
    }
    pub fn delay_nanos(&self) -> i64 {
        if !self.ok {
            i64::MAX
        } else {
            duration(self.at - self.reserved_at).max(0)
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitError {
    ZeroBurst,
    Deadline,
}
#[derive(Debug)]
pub struct RateLimiter {
    limit: f64,
    burst: i64,
    tokens: f64,
    last: i128,
    last_event: i128,
    latest: i128,
}
fn duration(nanos: i128) -> i64 {
    nanos.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}
fn seconds(nanos: i64) -> f64 {
    (nanos / SECOND) as f64 + (nanos % SECOND) as f64 / SECOND as f64
}
fn tokens_from_duration(rate: f64, nanos: i64) -> f64 {
    if rate <= 0.0 {
        0.0
    } else {
        seconds(nanos) * rate
    }
}
fn duration_from_tokens(rate: f64, tokens: f64) -> i64 {
    if rate <= 0.0 {
        i64::MAX
    } else {
        ((tokens / rate) * SECOND as f64) as i64
    }
}
impl RateLimiter {
    pub fn new(limit: f64, burst: i64) -> Self {
        Self {
            limit,
            burst,
            tokens: burst as f64,
            last: ZERO_TIME,
            last_event: ZERO_TIME,
            latest: ZERO_TIME,
        }
    }
    pub fn limit(&self) -> f64 {
        self.limit
    }
    pub fn burst(&self) -> i64 {
        self.burst
    }
    fn now(&mut self, now: i64) -> i128 {
        self.latest = self.latest.max(i128::from(now));
        self.latest
    }
    fn advance(&self, now: i128) -> f64 {
        let elapsed = duration(now - self.last.min(now));
        let tokens = self.tokens + tokens_from_duration(self.limit, elapsed);
        if tokens > self.burst as f64 {
            self.burst as f64
        } else {
            tokens
        }
    }
    pub fn tokens(&mut self, now: i64) -> f64 {
        let now = self.now(now);
        self.advance(now)
    }
    pub fn set_limit(&mut self, limit: f64) {
        self.tokens = self.advance(self.latest);
        self.last = self.latest;
        self.limit = limit;
    }
    pub fn set_burst(&mut self, burst: i64) {
        self.tokens = self.advance(self.latest);
        self.last = self.latest;
        self.burst = burst;
    }
    pub fn set_limit_and_burst(&mut self, limit: f64, burst: i64) {
        self.set_limit(limit);
        self.set_burst(burst);
    }
    fn reserve_raw(&mut self, now: i128, future: i64) -> Reservation {
        if self.limit == f64::MAX {
            return Reservation {
                ok: true,
                tokens: 1,
                at: now,
                reserved_at: now,
                rate: 0.0,
            };
        }
        let tokens = self.advance(now) - 1.0;
        let wait = if tokens < 0.0 {
            duration_from_tokens(self.limit, -tokens)
        } else {
            0
        };
        let ok = 1 <= self.burst && wait <= future;
        let mut reservation = Reservation {
            ok,
            tokens: 0,
            at: ZERO_TIME,
            reserved_at: now,
            rate: self.limit,
        };
        if ok {
            reservation.tokens = 1;
            reservation.at = now + i128::from(wait);
            self.last = now;
            self.tokens = tokens;
            self.last_event = reservation.at;
        }
        reservation
    }
    pub fn allow(&mut self, now: i64) -> bool {
        let now = self.now(now);
        self.reserve_raw(now, 0).ok
    }
    pub fn reserve(&mut self, now: i64) -> Reservation {
        let now = self.now(now);
        let mut reservation = self.reserve_raw(now, i64::MAX);
        if !reservation.ok || reservation.delay_nanos() != 0 {
            self.cancel_at(&reservation, now);
            reservation.ok = false;
        }
        reservation
    }
    /// Caller checks cancellation before this operation, then waits for the
    /// returned delay. On cancellation call cancel without advancing real time.
    pub fn reserve_wait(
        &mut self,
        now: i64,
        deadline: Option<i64>,
    ) -> Result<Reservation, WaitError> {
        let now = self.now(now);
        let reservation = self.reserve_raw(now, i64::MAX);
        let error = if !reservation.ok {
            Some(WaitError::ZeroBurst)
        } else if reservation.delay_nanos() > 0
            && deadline.is_some_and(|deadline| reservation.at > i128::from(deadline))
        {
            Some(WaitError::Deadline)
        } else {
            None
        };
        if let Some(error) = error {
            self.cancel_at(&reservation, now);
            Err(error)
        } else {
            Ok(reservation)
        }
    }
    pub fn used(&mut self, reservation: &Reservation, used: bool) {
        if !used {
            self.cancel(reservation);
        }
    }
    pub fn cancel(&mut self, reservation: &Reservation) {
        self.latest = self.latest.max(reservation.reserved_at);
        self.cancel_at(reservation, self.latest);
    }
    fn cancel_at(&mut self, reservation: &Reservation, now: i128) {
        if !reservation.ok
            || self.limit == f64::MAX
            || reservation.tokens == 0
            || reservation.at < now
        {
            return;
        }
        let restore = reservation.tokens as f64
            - tokens_from_duration(reservation.rate, duration(self.last_event - reservation.at));
        if restore <= 0.0 {
            return;
        }
        let tokens = self.advance(now) + restore;
        self.tokens = if tokens > self.burst as f64 {
            self.burst as f64
        } else {
            tokens
        };
        self.last = now;
        if reservation.at == self.last_event {
            let previous = reservation.at
                + i128::from(duration_from_tokens(
                    reservation.rate,
                    -(reservation.tokens as f64),
                ));
            if previous >= now {
                self.last_event = previous;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn clock_rate_original_go_operations() {
        let cases: serde_json::Value =
            serde_json::from_str(include_str!("../../../../tests/fixtures/clock-rate.json"))
                .unwrap();
        for case in cases.as_array().unwrap() {
            let mut limiter = RateLimiter::new(
                case["rate"].as_str().unwrap().parse().unwrap(),
                case["burst"].as_i64().unwrap(),
            );
            let mut reservations = HashMap::new();
            for step in case["steps"].as_array().unwrap() {
                let now = step["now"].as_i64().unwrap();
                let id = step["id"].as_str().unwrap_or_default();
                let mut allowed = false;
                let mut wait_error = "";
                let mut wait_delay = 0;
                match step["op"].as_str().unwrap() {
                    "allow" => allowed = limiter.allow(now),
                    "reserve" => {
                        let reservation = limiter.reserve(now);
                        allowed = reservation.allowed();
                        reservations.insert(id.to_owned(), reservation);
                    }
                    "used" => {
                        limiter.used(&reservations[id], step["used"].as_bool().unwrap_or(false))
                    }
                    "check" => allowed = reservations[id].allowed(),
                    "limit" => limiter.set_limit(step["rate"].as_str().unwrap().parse().unwrap()),
                    "burst" => limiter.set_burst(step["burst"].as_i64().unwrap_or(0)),
                    "both" => limiter.set_limit_and_burst(
                        step["rate"].as_str().unwrap().parse().unwrap(),
                        step["burst"].as_i64().unwrap_or(0),
                    ),
                    "tokens" => {
                        limiter.tokens(now);
                    }
                    "wait" => match limiter.reserve_wait(now, step["deadline"].as_i64()) {
                        Ok(reservation) => {
                            wait_delay = reservation.delay_nanos();
                            wait_error = "ok";
                            if step["interleave"].as_bool().unwrap_or(false) {
                                limiter.tokens(now + step["wait_advance"].as_i64().unwrap_or(0));
                            }
                            if step["wait_cancel"].as_bool().unwrap_or(false) {
                                limiter.cancel(&reservation);
                                wait_error = "cancelled";
                            }
                        }
                        Err(WaitError::ZeroBurst) => wait_error = "zero",
                        Err(WaitError::Deadline) => wait_error = "deadline",
                    },
                    other => panic!("unknown operation {other}"),
                }
                assert_eq!(
                    wait_error,
                    step["wait_error"].as_str().unwrap_or_default(),
                    "{case}"
                );
                assert_eq!(
                    wait_delay,
                    step["wait_delay"].as_i64().unwrap_or(0),
                    "{case}"
                );
                assert_eq!(allowed, step["allowed"].as_bool().unwrap(), "{case}");
                assert_eq!(
                    limiter.advance(limiter.latest),
                    step["tokens"].as_str().unwrap().parse::<f64>().unwrap(),
                    "{case}"
                );
                assert_eq!(
                    limiter.limit(),
                    step["limit"].as_str().unwrap().parse::<f64>().unwrap(),
                    "{case}"
                );
                assert_eq!(
                    limiter.burst(),
                    step["actual_burst"].as_i64().unwrap(),
                    "{case}"
                );
                assert_eq!(
                    limiter.latest == ZERO_TIME,
                    step["latest_zero"].as_bool().unwrap(),
                    "{case}"
                );
                if limiter.latest != ZERO_TIME {
                    assert_eq!(
                        limiter.latest,
                        i128::from(step["latest"].as_i64().unwrap()),
                        "{case}"
                    );
                }
            }
        }
    }
}
