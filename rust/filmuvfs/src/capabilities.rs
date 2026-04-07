use crate::config::MountAdapterKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryCapabilities {
    pub default_mount_adapter: MountAdapterKind,
    pub supported_mount_adapters: Vec<MountAdapterKind>,
    pub windows_projfs_compiled: bool,
    pub windows_winfsp_compiled: bool,
}

impl BinaryCapabilities {
    #[must_use]
    pub fn detect() -> Self {
        let mut supported_mount_adapters = vec![MountAdapterKind::Auto];

        #[cfg(target_os = "linux")]
        {
            supported_mount_adapters.push(MountAdapterKind::Fuse);
        }

        #[cfg(target_os = "windows")]
        {
            supported_mount_adapters.push(MountAdapterKind::Projfs);
            if crate::windows_winfsp::winfsp_backend_compiled() {
                supported_mount_adapters.push(MountAdapterKind::Winfsp);
            }
        }

        Self {
            default_mount_adapter: MountAdapterKind::default_for_platform(),
            supported_mount_adapters,
            windows_projfs_compiled: cfg!(target_os = "windows"),
            windows_winfsp_compiled: crate::windows_winfsp::winfsp_backend_compiled(),
        }
    }

    #[must_use]
    pub fn to_json(&self) -> String {
        let adapters = self
            .supported_mount_adapters
            .iter()
            .map(|adapter| format!("\"{}\"", adapter.as_str()))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            concat!(
                "{{",
                "\"default_mount_adapter\":\"{}\",",
                "\"supported_mount_adapters\":[{}],",
                "\"windows_projfs_compiled\":{},",
                "\"windows_winfsp_compiled\":{}",
                "}}"
            ),
            self.default_mount_adapter.as_str(),
            adapters,
            self.windows_projfs_compiled,
            self.windows_winfsp_compiled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::BinaryCapabilities;
    use crate::config::MountAdapterKind;

    #[test]
    fn serializes_capabilities_to_json() {
        let capabilities = BinaryCapabilities {
            default_mount_adapter: MountAdapterKind::Auto,
            supported_mount_adapters: vec![MountAdapterKind::Auto, MountAdapterKind::Projfs],
            windows_projfs_compiled: true,
            windows_winfsp_compiled: false,
        };

        let json = capabilities.to_json();
        assert!(json.contains("\"default_mount_adapter\":\"auto\""));
        assert!(json.contains("\"supported_mount_adapters\":[\"auto\",\"projfs\"]"));
        assert!(json.contains("\"windows_projfs_compiled\":true"));
        assert!(json.contains("\"windows_winfsp_compiled\":false"));
    }
}
