package org.agmip.acmo.translators;

import java.io.File;

public interface AcmoTranslator {
    public File execute(String sourceFolder, String destFolder);
}
