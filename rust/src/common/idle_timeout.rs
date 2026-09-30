use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Handle;
use tokio::time::{Instant, Sleep};

pub(crate) struct IdleTimeoutReader<R> {
    inner: R,
    timeout: Option<(Duration, Handle)>,
    waiting_since: Option<Instant>,
    idle: Option<Pin<Box<Sleep>>>,
}

impl<R> IdleTimeoutReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            timeout: None,
            waiting_since: None,
            idle: None,
        }
    }

    pub(crate) fn set_timeout(&mut self, timeout: Option<(Duration, Handle)>) {
        self.timeout = timeout;
        self.waiting_since = None;
        self.idle = None;
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for IdleTimeoutReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if let Poll::Ready(result) = Pin::new(&mut this.inner).poll_read(cx, buf) {
            this.waiting_since = None;
            return Poll::Ready(result);
        }

        let Some((timeout, handle)) = &this.timeout else {
            return Poll::Pending;
        };

        let deadline = *this.waiting_since.get_or_insert_with(Instant::now) + *timeout;
        let idle = this.idle.get_or_insert_with(|| {
            let _runtime = handle.enter();
            Box::pin(tokio::time::sleep_until(deadline))
        });
        while idle.as_mut().poll(cx).is_ready() {
            if Instant::now() >= deadline {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("No data received for {}ms", timeout.as_millis()),
                )));
            }
            idle.as_mut().reset(deadline);
        }
        Poll::Pending
    }
}
