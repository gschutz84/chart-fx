package de.gsi.chart.samples.utils.css;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.ObservableList;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JavaFx Control, which allows applying custom css entered in a TextField to a scene/control
 *
 * @author Alexander Krimm
 */
public class CssEditor extends VBox {
    private static final Logger LOGGER = LoggerFactory.getLogger(CssEditor.class);
    private static final String CSS_POSTFIX = "Custom.css";
    private static final String CSS_PROTOCOL = "css:";
    private String defaultCss = "";
    private ObjectProperty<String> customCss = new SimpleObjectProperty<>(this, "customCss", defaultCss);

    public CssEditor(final Scene scene, final String name, final String defaultCss, final double width) {
        super();
        this.defaultCss = defaultCss;
        Handler.registerStyleSheet(name + CSS_POSTFIX, customCss);
        final ObservableList<String> stylesheets = scene.getStylesheets();
        stylesheets.add(CSS_PROTOCOL + name + CSS_POSTFIX);

        final ComboBox<String> files = new ComboBox<>();
        files.setPrefWidth(width);
        files.setMaxWidth(width);
        VBox.setVgrow(files, Priority.NEVER);
        final TextArea editor = new TextArea();
        editor.setPrefWidth(width);
        editor.setMaxWidth(width);
        editor.setEditable(false);
        VBox.setVgrow(editor, Priority.ALWAYS);
        final Button reset = new Button("Reset to default values");
        HBox.setHgrow(reset, Priority.ALWAYS);
        final Button apply = new Button("Apply");
        HBox.setHgrow(apply, Priority.ALWAYS);
        final HBox buttons = new HBox(reset, apply);
        VBox.setVgrow(buttons, Priority.NEVER);
        getChildren().addAll(files, editor, buttons);
        files.setItems(stylesheets);
        files.valueProperty().addListener((prop, oldVal, newVal) -> {
            URI uri;
            try {
                if (newVal == null || newVal.isBlank()) {
                    editor.textProperty().unbindBidirectional(customCss);
                    editor.setText("");
                    editor.setEditable(false);
                    reset.setDisable(true);
                    apply.setDisable(true);
                    return;
                }
                uri = new URI(newVal);
                if (uri.getScheme().equals("file")) {
                    editor.textProperty().unbind();
                    Path path = Paths.get(uri);
                    editor.setText(Files.readString(path, StandardCharsets.UTF_8));
                    editor.setEditable(false);
                    reset.setDisable(true);
                    apply.setDisable(true);
                } else if (uri.getScheme().equals("css")) {
                    editor.textProperty().bindBidirectional(customCss);
                    editor.setEditable(true);
                    reset.setDisable(false);
                    apply.setDisable(false);
                } else {
                    throw new UnsupportedOperationException("cannot handle url scheme: " + uri.getScheme());
                }
            } catch (URISyntaxException | IOException e) {
                LOGGER.atError().setCause(e).log("Error reading css file");
            }
        });
        apply.setOnAction(evt -> {
            int selIndex = files.getSelectionModel().getSelectedIndex();
            int index = scene.getStylesheets().indexOf(files.getValue());
            scene.getStylesheets().set(index, files.getValue());
            files.getSelectionModel().select(selIndex);
            // todo: trigger callback for repaint or trigger repaint for whole scene
        });
        reset.setOnAction(evt -> {
            editor.setText(defaultCss);
            apply.getOnAction().handle(evt);
        });
        files.getSelectionModel().select(0);
    }

    public CssEditor(final Parent parent, final String name, final String defaultCss, final double width) {
        super();
        this.defaultCss = defaultCss;
        Handler.registerStyleSheet(name + CSS_POSTFIX, customCss);
        final ObservableList<String> stylesheets = parent.getStylesheets();
        stylesheets.add(CSS_PROTOCOL + name + CSS_POSTFIX);

        final ComboBox<String> files = new ComboBox<>();
        files.setPrefWidth(width);
        files.setMaxWidth(width);
        VBox.setVgrow(files, Priority.NEVER);
        final TextArea editor = new TextArea();
        editor.setPrefWidth(width);
        editor.setMaxWidth(width);
        editor.setEditable(false);
        VBox.setVgrow(editor, Priority.ALWAYS);
        final Button reset = new Button("Reset to default values");
        HBox.setHgrow(reset, Priority.ALWAYS);
        final Button apply = new Button("Apply");
        HBox.setHgrow(apply, Priority.ALWAYS);
        final HBox buttons = new HBox(reset, apply);
        VBox.setVgrow(buttons, Priority.NEVER);
        getChildren().addAll(files, editor, buttons);
        files.setItems(stylesheets);
        files.valueProperty().addListener((prop, oldVal, newVal) -> {
            URI uri;
            try {
                if (newVal == null || newVal.isBlank()) {
                    editor.textProperty().unbindBidirectional(customCss);
                    editor.setText("");
                    editor.setEditable(false);
                    reset.setDisable(true);
                    apply.setDisable(true);
                    return;
                }
                uri = new URI(newVal);
                if (uri.getScheme().equals("file")) {
                    editor.textProperty().unbind();
                    Path path = Paths.get(uri);
                    editor.setText(Files.readString(path, StandardCharsets.UTF_8));
                    editor.setEditable(false);
                    reset.setDisable(true);
                    apply.setDisable(true);
                } else if (uri.getScheme().equals("css")) {
                    editor.textProperty().bindBidirectional(customCss);
                    editor.setEditable(true);
                    reset.setDisable(false);
                    apply.setDisable(false);
                } else {
                    throw new UnsupportedOperationException("cannot handle url scheme: " + uri.getScheme());
                }
            } catch (URISyntaxException | IOException e) {
                LOGGER.atError().setCause(e).log("Error reading css file");
            }
        });
        apply.setOnAction(evt -> {
            int selIndex = files.getSelectionModel().getSelectedIndex();
            int index = parent.getStylesheets().indexOf(files.getValue());
            parent.getStylesheets().set(index, files.getValue());
            files.getSelectionModel().select(selIndex);
            // todo: trigger callback for repaint or trigger repaint for whole scene
        });
        reset.setOnAction(evt -> {
            editor.setText(defaultCss);
            apply.getOnAction().handle(evt);
        });
        files.getSelectionModel().select(0);
    }
}
