package com.amazon.kinesis.kafka.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class ConfigParser {

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);

    private ConfigParser() { }

    private static File loadAsResource(String fileUrl) {
        File configFile = null;
        try {
            URL url = ConfigParser.class.getClassLoader().getResource(fileUrl);
            if (url != null) {
                Path path = Paths.get(url.toURI());
                configFile = path.toFile();
            }
        } catch (URISyntaxException e) {
            log.warn("There was an error loading the file as a classpath resource. Please ensure fileUrl {} is valid.", fileUrl);
        }

        return configFile;
    }

    private static File load(String fileUrl) {
        File configFile = loadAsResource(fileUrl);
        if (configFile == null || !Files.exists(configFile.toPath())) {
            log.info("File could not be loaded as a resource. Will attempt to open directly.");
            configFile = new File(fileUrl);
        }
        return configFile;
    }

    public static Optional<ClusterMapping> parse(String fileUrl) {
        ClusterMapping mapping = null;
        File configFile = load(fileUrl);

        try(FileInputStream fis = new FileInputStream(configFile);
            BufferedInputStream bis = new BufferedInputStream(fis)) {
            Yaml yaml = new Yaml();
            mapping = yaml.loadAs(bis, ClusterMapping.class);
        } catch (FileNotFoundException fnfe) {
            log.error("File {} could not be found. Please ensure the file is in the correct location.", fileUrl);
        } catch (IOException ioe) {
            log.error("There was an error trying to read file ().", fileUrl);
        } catch (YAMLException ye) {
            log.error("File {} is not valid YAML or it does not conform to the expected structure required.", fileUrl);
        }

        return Optional.ofNullable(mapping);
    }
}
