package store.ldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static store.ldb.Utils.requireTrue;

class Manifest {
    public static final Logger LOG = LoggerFactory.getLogger(Manifest.class);

    private final Set<String> segments = new HashSet<>();
    private final PrintWriter writer;

    public Manifest(String dirName) {
        File dir = new File(dirName);
        File file = new File(manifestFilename(dirName));
        try {
            if (!dir.exists() && !dir.mkdirs()) throw new RuntimeException("couldn't create dir: " + dirName);
            if (!file.exists()) requireTrue(file.createNewFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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

        try {
            this.writer = new PrintWriter(new BufferedWriter(new FileWriter(manifestFilename(dirName), true)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String manifestFilename(String dirName) {
        return dirName + File.separatorChar + "manifest";
    }

    public synchronized boolean contains(Segment segment) {
        return segments.contains(segment.fileName);
    }

    public synchronized void record(List<Segment> created, List<Segment> deleted) {
        for (Segment segment : created) {
            writer.println("+" + segment.fileName);
        }
        for (Segment segment : deleted) {
            writer.println("-" + segment.fileName);
        }
        writer.flush();
    }

    public void close() {
        writer.flush();
        writer.close();
    }
}
