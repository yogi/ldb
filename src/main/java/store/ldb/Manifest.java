package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static store.ldb.Utils.requireTrue;

class Manifest {
    public static final Logger LOG = LoggerFactory.getLogger(Manifest.class);

    private final File dir;
    private final Set<String> segments = new HashSet<>();
    private PrintWriter writer;
    private final File manifestFile;
    private AtomicInteger linesAdded = new AtomicInteger();

    public Manifest(String dirName) {
        dir = new File(dirName);
        manifestFile = new File(manifestFilename(dirName));

        // init dir
        try {
            if (!dir.exists() && !dir.mkdirs()) throw new RuntimeException("couldn't create dir: " + dirName);
            if (!manifestFile.exists()) requireTrue(manifestFile.createNewFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // load manifest
        try {
            BufferedReader reader = new BufferedReader(new FileReader(manifestFilename(dirName)));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("+")) {
                    segments.add(line.substring(1));
                } else if (line.startsWith("-")) {
                    segments.remove(line.substring(1));
                } else {
                    LOG.error("ignoring invalid manifest entry: " + line);
                }
            }
        } catch (IOException e) {
            LOG.error("ignoring exception loading manifest since it might be due to it not getting flushed in a crash: ", e);
        }

        // init writer
        try {
            this.writer = new PrintWriter(new BufferedWriter(new FileWriter(manifestFilename(dirName), true)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String manifestFilename(String dirName) {
        return dirName + File.separatorChar + "manifest";
    }

    private String snapshotFilename(String dirName) {
        return dirName + File.separatorChar + "manifest";
    }

    public synchronized boolean contains(Segment segment) {
        return segments.contains(segment.fileName);
    }

    public synchronized void record(List<Segment> created, List<Segment> deleted) {
        for (Segment segment : created) {
            writer.println("+" + segment.fileName);
            segments.add(segment.fileName);
            linesAdded.incrementAndGet();
        }
        for (Segment segment : deleted) {
            writer.println("-" + segment.fileName);
            segments.remove(segment.fileName);
            linesAdded.incrementAndGet();
        }
        writer.flush();

        if (linesAdded.get() > 1000) {
            LOG.info("rewriting manifest");
            linesAdded.set(0);
            File newManifest = new File(snapshotFilename(dir.getPath()));
            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(newManifest)))) {
                for (String s : segments) {
                    pw.println("+" + s);
                }
            } catch (IOException e) {
                LOG.error("ignoring error when trying to rewrite manifest file", e);
            }

            writer.flush();
            writer.close();
            requireTrue(newManifest.renameTo(manifestFile));
            try {
                writer = new PrintWriter(new BufferedWriter(new FileWriter(manifestFilename(dir.getPath()), true)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        writer.flush();
        writer.close();
    }
}
