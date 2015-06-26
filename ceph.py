#!/usr/bin/env python
#
# vim: tabstop=4 shiftwidth=4

# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; only version 2 of the License is applicable.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
#
# Authors:
#   Ricardo Rocha <ricardo@catalyst.net.nz>
#
# About this plugin:
#   Helper object for all plugins.
#
# collectd:
#   http://collectd.org
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml
#

import collectd
import traceback
import pprint
from ceph_pool_plugin       import CephPoolPlugin
from ceph_pg_plugin         import CephPGPlugin
from ceph_latency_plugin    import CephLatencyPlugin
from ceph_monitor_plugin        import CephMonPlugin
from ceph_osd_plugin        import CephOsdPlugin

class Ceph(object):

    def __init__(self):
        self.plugins = [CephLatencyPlugin(),CephPoolPlugin(),CephPGPlugin(),CephMonPlugin(),CephOsdPlugin()]
        #self.plugins = [CephOsdPlugin()]
        self.verbose = False
        self.prefix = 'ceph'
        self.cluster = 'ceph'
        self.testpool = 'test'
        self.interval = None

    def config_callback(self, conf):
        """Takes a collectd conf object and fills in the local config."""
        for node in conf.children:
            if node.key == "Verbose":
                if node.values[0] in ['True', 'true']:
                    self.verbose = True
            elif node.key == "Prefix":
                self.prefix = node.values[0]
            elif node.key == 'Cluster':
                self.cluster = node.values[0]
            elif node.key == 'TestPool':
                self.testpool = node.values[0]
            elif node.key == 'Interval':
                self.interval = int(node.values[0])
            else:
                collectd.warning("%s: unknown config key: %s" % (self.prefix, node.key))
        
        args = [self.verbose,self.prefix,self.cluster,self.testpool,self.interval]
        for plugin in self.plugins:
            plugin.config(*args)

    def dispatch(self, stats):
        """
        Dispatches the given stats.

        stats should be something like:

        {'plugin': {'plugin_instance': {'type': {'type_instance': <value>, ...}}}}
        """
        if not stats:
            collectd.error("%s: failed to retrieve stats" % self.prefix)
            return

        self.logverbose("dispatching %d new stats :: %s" % (len(stats), stats))
        try:
            for plugin in stats.keys():
                for plugin_instance in stats[plugin].keys():
                    for type in stats[plugin][plugin_instance].keys():
                        type_value = stats[plugin][plugin_instance][type]
                        if not isinstance(type_value, dict):
                            self.dispatch_value(plugin, plugin_instance, type, None, type_value)
                        else:
                          for type_instance in stats[plugin][plugin_instance][type].keys():
                              self.logverbose("will dispatch with type instance %s" % ",".join(stats[plugin][plugin_instance][type].keys()))
                              self.dispatch_value(plugin, plugin_instance,
                                      type, type_instance,
                                      stats[plugin][plugin_instance][type][type_instance])
        except Exception as exc:
            collectd.error("%s: failed to dispatch values :: %s :: %s"
                    % (self.prefix, exc, traceback.format_exc()))

    def dispatch_value(self, plugin, plugin_instance, type, type_instance, value):
        """Looks for the given stat in stats, and dispatches it"""
        #self.logverbose("dispatching value %s.%s.%s.%s=%s"
                #% (plugin, plugin_instance, type, type_instance, value))

        val = collectd.Values(type='gauge')
        val.plugin=plugin
        val.plugin_instance=plugin_instance
        if type_instance is not None:
            val.type_instance="%s-%s" % (type, type_instance)
        else:
            val.type_instance=type
        val.values=[value]
        val.interval = self.interval
        val.dispatch()
        self.logverbose("sent metric %s.%s.%s.%s.%s" 
                % (plugin, plugin_instance, type, type_instance, value))

    def read_callback(self):
        try:
            self.logverbose("Read callback will execute ...")
            stats = self.get_stats()
            for stat in stats:
                self.dispatch(stat)
        except Exception as exc:
            collectd.error("%s: failed to get stats :: %s :: %s"
                    % (self.prefix, exc, traceback.format_exc()))

    def get_stats(self):
        allstats = []
        for plugin in self.plugins:
            pluginStat = plugin.get_stats()
            self.logverbose("plugin %s stats : %s" % (plugin.name,pluginStat))
            allstats.append(pluginStat)
            #allstats = self.merge_stats(allstats,pluginStat)
        #self.logverbose("all stats : %s" % allstats)
        return allstats

    def logverbose(self, msg):
        if self.verbose:
            collectd.info("%s: %s" % (self.prefix, msg))

    def merge_stats(self,s1,s2):
        ss1,ss2 = s1[self.cluster],s2[self.cluster]
        merged = ss1.copy()
        merged.update(ss2) 
        return {self.cluster : merged }
    

try:
    plugin = Ceph()
except Exception as exc:
    collectd.error("ceph-pool: failed to initialize ceph pool plugin :: %s :: %s"
            % (exc, traceback.format_exc()))

def configure_callback(conf):
    """Received configuration information"""
    plugin.config_callback(conf)

def read_callback():
    """Callback triggerred by collectd on read"""
    plugin.read_callback()

collectd.register_config(configure_callback)
collectd.register_read(read_callback)

