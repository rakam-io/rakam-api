package org.rakam.ui;

import com.google.auto.service.AutoService;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.rakam.plugin.RakamModule;
import org.rakam.server.http.HttpService;

/**
 * Created by buremba <Burak Emre Kabakcı> on 16/04/15 21:21.
 */
@AutoService(RakamModule.class)
public class RakamUIModule extends RakamModule {
    @Override
    protected void setup(Binder binder) {
        Multibinder<HttpService> httpServices = Multibinder.newSetBinder(binder, HttpService.class);
        httpServices.addBinding().to(ReportHttpService.class);

    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }
}
