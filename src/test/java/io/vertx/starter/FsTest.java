package io.vertx.starter;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.vertx.core.file.OpenOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
class FsTest {

  @Test
  public void flowSaveWorks() throws IOException {
    Vertx vertx = Vertx.vertx();
    for (int i = 0; i < 10_000; i++) {
      final String hello = "hello-world!!!";
      final Flowable<Byte> flow = Flowable.fromArray(ArrayUtils.toObject(hello.getBytes()));
      final Path file = Files.createTempFile("hello", ".txt");
      file.toFile().delete();
      vertx.fileSystem().rxOpen(file.toString(), new OpenOptions().setWrite(true))
        .flatMapCompletable(asyncFile ->
          Completable.create(
            emitter ->
              flow.buffer(1024)
                .map(bytes -> Buffer.buffer(ArrayUtils.toPrimitive(bytes.toArray(new Byte[0]))))
                // does onComplete action is called when file is written?
                .subscribe(asyncFile.toSubscriber().onWriteStreamEnd(emitter::onComplete))
          )
        ).blockingAwait();
      assertEquals(new String(Files.readAllBytes(file)), hello);
    }
    vertx.close();
  }
}
