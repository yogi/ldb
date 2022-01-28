package app;

import java.io.ObjectOutputStream;

public interface Cmd {
    void writeTo(ObjectOutputStream os);
}
