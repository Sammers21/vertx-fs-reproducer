package io.vertx.starter;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ExtendWith(VertxExtension.class)
class FsTest {

  @Test
  public void flowSaveWorks() throws IOException {
    Vertx vertx = Vertx.vertx();
    final AtomicLong counter = new AtomicLong(40);
    final Flowable<Buffer> flow = Flowable.generate(emitter -> {
      System.out.println("Counter: " + counter.get());
      if (counter.decrementAndGet() == 0) {
        return;
      } else {
        // 8kb
        emitter.onNext(Buffer.buffer(new byte[1024 * 8]));
      }
    });
    final Path file = Files.createTempFile("hello", ".txt");
    file.toFile().delete();
    vertx.fileSystem().rxOpen(file.toString(), new OpenOptions().setWrite(true))
      .flatMapCompletable(asyncFile ->
        Completable.create(
          emitter ->
            flow.subscribe(asyncFile.toSubscriber().onWriteStreamEnd(emitter::onComplete))
        )
      ).blockingGet(10, TimeUnit.SECONDS);
    vertx.close();
  }

  @Test
  public void flowReadWorks() throws IOException {
    Vertx vertx = Vertx.vertx();
    final Path temp = Files.createTempFile("hello", ".txt");
    Files.write(temp, "123".getBytes());
    vertx.fileSystem()
      .rxOpen(temp.toString(), new OpenOptions().setRead(true))
      .flatMapPublisher(asyncFile -> asyncFile.toFlowable().mergeWith(asyncFile.rxClose()))
      .toList()
      .blockingGet();
    vertx.close();
  }

}
