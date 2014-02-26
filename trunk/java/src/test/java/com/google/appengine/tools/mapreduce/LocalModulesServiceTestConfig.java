package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.modules.ModulesServicePb.GetDefaultVersionRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetDefaultVersionResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetHostnameRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetHostnameResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetModulesRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetModulesResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetNumInstancesRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetNumInstancesResponse;
import com.google.appengine.api.modules.ModulesServicePb.GetVersionsRequest;
import com.google.appengine.api.modules.ModulesServicePb.GetVersionsResponse;
import com.google.appengine.api.modules.ModulesServicePb.ModulesServiceError.ErrorCode;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesRequest;
import com.google.appengine.api.modules.ModulesServicePb.SetNumInstancesResponse;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StartModuleResponse;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleRequest;
import com.google.appengine.api.modules.ModulesServicePb.StopModuleResponse;
import com.google.appengine.tools.development.AbstractLocalRpcService;
import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.LocalRpcService;
import com.google.appengine.tools.development.LocalServiceContext;
import com.google.appengine.tools.development.testing.LocalServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

// TODO(user): This needs to be replaced once b/8321405 is fixed and released.
/**
 * Dummy (a hack) support for ModulesService.
 */
public class LocalModulesServiceTestConfig implements LocalServiceTestConfig {

  /**
   * Version information for a module.
   */
  public static class VersionInfo {

    private final String name;
    private final int instances;

    public VersionInfo(String name, int instances) {
      if (name == null || name.isEmpty()) {
        throw new RuntimeException("version name must not be null or empty");
      }
      this.name = name;
      this.instances = instances;
    }
  }

  /**
   * Module information.
   */
  public static class ModuleInfo {

    private final String name;
    private final VersionInfo defaulVersion;
    private final Map<String, VersionInfo> versions = new LinkedHashMap<>();

    public ModuleInfo(String name, VersionInfo defaultVersion, VersionInfo... otherVersions) {
      if (name == null || name.isEmpty()) {
        throw new RuntimeException("module name must not be null or empty");
      }
      this.name = name;
      this.defaulVersion = defaultVersion;
      for (VersionInfo version : otherVersions) {
        versions.put(version.name, version);
      }
    }

    VersionInfo getVersion(String version) {
      if (defaulVersion.name.equals(version)) {
        return defaulVersion;
      }
      VersionInfo versionInfo = versions.get(version);
      if (versionInfo == null) {
        throw new ApiProxy.ApplicationException(ErrorCode.INVALID_VERSION_VALUE);
      }
      return versionInfo;
    }
  }

  /**
   * Dummy implementation for ModulesService, used for testing only.
   */
  public static class DummyModulesService extends AbstractLocalRpcService {

    private static final String PACKAGE = "modules";

    private final String hostname;
    private final ModuleInfo defaultModule;
    private final Map<String, ModuleInfo> modules = new HashMap<>();

    private DummyModulesService(String hostname, ModuleInfo defaultModule, ModuleInfo... modules) {
      this.hostname = hostname;
      this.defaultModule = defaultModule;
      this.modules.put(defaultModule.name, defaultModule);
      for (ModuleInfo module : modules) {
        this.modules.put(module.name, module);
      }
    }

    private ModuleInfo getDefaultModule() {
      return defaultModule;
    }

    @Override
    public String getPackage() {
      return PACKAGE;
    }

    private ModuleInfo getModule(String name) {
      ModuleInfo module = modules.get(name);
      if (module == null) {
        throw new ApiProxy.ApplicationException(ErrorCode.INVALID_MODULE_VALUE);
      }
      return module;
    }

    @SuppressWarnings("unused")
    public GetModulesResponse getModules(Status status, GetModulesRequest request) {
      GetModulesResponse.Builder result = GetModulesResponse.newBuilder();
      for (String module : modules.keySet()) {
        result.addModule(module);
      }
      status.setSuccessful(true);
      return result.build();
    }

    public GetVersionsResponse getVersions(Status status, GetVersionsRequest request) {
      status.setSuccessful(false);
      GetVersionsResponse.Builder result = GetVersionsResponse.newBuilder();
      ModuleInfo module = getModule(request.getModule());
      result.addVersion(module.defaulVersion.name);
      for (String version : module.versions.keySet()) {
        result.addVersion(version);
      }
      status.setSuccessful(true);
      return result.build();
    }

    public GetDefaultVersionResponse getDefaultVersion(
        Status status, GetDefaultVersionRequest request) {
      status.setSuccessful(false);
      GetDefaultVersionResponse.Builder result = GetDefaultVersionResponse.newBuilder();
      ModuleInfo module = getModule(request.getModule());
      result.setVersion(module.defaulVersion.name);
      status.setSuccessful(true);
      return result.build();
    }

    public GetNumInstancesResponse getNumInstances(Status status, GetNumInstancesRequest request) {
      status.setSuccessful(false);
      GetNumInstancesResponse.Builder result = GetNumInstancesResponse.newBuilder();
      ModuleInfo module = getModule(request.getModule());
      VersionInfo version = module.getVersion(request.getVersion());
      result.setInstances(version.instances);
      status.setSuccessful(true);
      return result.build();
    }

    @SuppressWarnings("unused")
    public SetNumInstancesResponse setNumInstances(Status status, SetNumInstancesRequest request) {
      status.setSuccessful(false);
      throw new UnsupportedOperationException(
          "ModulesService.setNumInstances not currentlysupported");
    }

    @SuppressWarnings("unused")
    public StartModuleResponse startModule(Status status, StartModuleRequest request) {
      status.setSuccessful(false);
      throw new UnsupportedOperationException(
          "ModulesService.setNumInstances not currentlysupported");
    }

    @SuppressWarnings("unused")
    public StopModuleResponse stopModule(Status status, StopModuleRequest request) {
      status.setSuccessful(false);
      throw new UnsupportedOperationException(
          "ModulesService.stopModule not currentlysupported");
    }

    public GetHostnameResponse getHostname(Status status, GetHostnameRequest request) {
      status.setSuccessful(false);
      GetHostnameResponse.Builder result = GetHostnameResponse.newBuilder();
      ModuleInfo module = request.hasModule() ? getModule(request.getModule()) : getDefaultModule();
      if (request.hasVersion()) {
        VersionInfo version = module.getVersion(request.getVersion());
        if (request.hasInstance()) {
          int instance = Integer.parseInt(request.getInstance());
          if (instance <= 0 || instance > version.instances) {
            throw new ApiProxy.ApplicationException(ErrorCode.INVALID_INSTANCES_VALUE);
          }
        }
      }
      result.setHostname(hostname);
      status.setSuccessful(true);
      return result.build();
    }
  }

  private LocalRpcService modulesService;
  private ModuleInfo[] modules;

  public LocalModulesServiceTestConfig(ModuleInfo... modules) {
    this.modules = modules;
  }

  @Override
  public void setUp() {
    Environment env = ApiProxy.getCurrentEnvironment();
    ModuleInfo defaultModule = new ModuleInfo("default", new VersionInfo("1", 1));
    ModuleInfo[] modules = new ModuleInfo[this.modules.length + 2];
    modules[0] = new ModuleInfo(env.getModuleId(), new VersionInfo("1", 1));
    modules[1] = new ModuleInfo(env.getAppId(), new VersionInfo(env.getVersionId(), 1));
    System.arraycopy(this.modules, 0, modules, 2, this.modules.length);
    try {
      ApiProxyLocal apiProxyLocal = LocalServiceTestHelper.getApiProxyLocal();
      Class<? extends ApiProxyLocal> apiProxyClass = apiProxyLocal.getClass();
      Field contextField = apiProxyClass.getDeclaredField("context");
      contextField.setAccessible(true);
      LocalServiceContext context = (LocalServiceContext) contextField.get(apiProxyLocal);
      Field serviceCacheField = apiProxyClass.getDeclaredField("serviceCache");
      serviceCacheField.setAccessible(true);
      Map<String, LocalRpcService> serviceCache =
          (Map<String, LocalRpcService>) serviceCacheField.get(apiProxyLocal);
      modulesService = serviceCache.put(DummyModulesService.PACKAGE, new DummyModulesService(
          context.getLocalServerEnvironment().getHostName(), defaultModule, modules));
    } catch (Exception ex) {
      throw new RuntimeException("Failed to initialize Fake modules service", ex);
    }
  }

  @Override
  public void tearDown() {
    try {
      ApiProxyLocal apiProxyLocal = LocalServiceTestHelper.getApiProxyLocal();
      Field serviceCacheField = apiProxyLocal.getClass().getDeclaredField("serviceCache");
      serviceCacheField.setAccessible(true);
      Map<String, LocalRpcService> serviceCache =
          (Map<String, LocalRpcService>) serviceCacheField.get(apiProxyLocal);
      if (modulesService != null) {
        modulesService = serviceCache.put(DummyModulesService.PACKAGE, modulesService);
      } else {
        serviceCache.remove(DummyModulesService.PACKAGE);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to reset original modules service", ex);
    }
  }
}
