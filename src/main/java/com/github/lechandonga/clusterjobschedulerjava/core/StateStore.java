package com.github.lechandonga.clusterjobschedulerjava.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * 基于单个本地 JSON 文件的状态存储。
 * 写入采用「写临时文件 + 原子 move」，保证快照要么是旧版本要么是新版本，不会出现半截文件。
 * 实际写入串行化由调度器的全局锁保证，这里再加一把锁做双保险。
 */
public class StateStore {

    private final Path file;
    private final ObjectMapper mapper;
    private final ReentrantLock fileLock = new ReentrantLock();

    public StateStore(Path file) {
        this.file = file;
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    }

    public Path getFile() {
        return file;
    }

    public void save(SchedulerSnapshot snapshot) {
        fileLock.lock();
        try {
            Path parent = file.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Path tmp = parent.resolve(file.getFileName() + ".tmp-" + Thread.currentThread().getId());
            byte[] bytes = mapper.writeValueAsBytes(snapshot);
            Files.write(tmp, bytes);
            try {
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("持久化调度器状态失败: " + file, e);
        } finally {
            fileLock.unlock();
        }
    }

    /** 首次启动（无文件）返回 null，由调度器建立空状态。 */
    public SchedulerSnapshot load() {
        fileLock.lock();
        try {
            if (!Files.exists(file)) {
                return null;
            }
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length == 0) {
                return null;
            }
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), SchedulerSnapshot.class);
        } catch (IOException e) {
            throw new UncheckedIOException("读取调度器状态失败: " + file, e);
        } finally {
            fileLock.unlock();
        }
    }
}
