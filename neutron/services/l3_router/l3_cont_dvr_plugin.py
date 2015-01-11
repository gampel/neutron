# Copyright (c) 2014 OpenStack Foundation.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


import threading

from ryu.base.app_manager import AppManager
from ryu.controller.ofp_handler import OFPHandler

from oslo.config import cfg
from oslo import messaging
from oslo.utils import excutils
from oslo.utils import importutils

from neutron import context
from neutron import manager

from neutron.api.rpc.agentnotifiers import l3_rpc_agent_api
from neutron.api.rpc.handlers import l3_rpc
from neutron.common import constants as q_const
from neutron.common import rpc as n_rpc
from neutron.common import topics
from neutron.common import utils
from neutron.plugins.common import constants

from neutron.db import common_db_mixin
from neutron.db import l3_gwmode_db
from neutron.db import l3_hascheduler_db

from neutron.openstack.common import log as logging
from neutron.openstack.common import loopingcall

from neutron.services.l3_router.l3_reactive_app import L3ReactiveApp

LOG = logging.getLogger(__name__)


NET_CONTROL_L3_OPTS = [
    cfg.StrOpt('L3controller_ip_list',
               default='tcp:10.100.100.38:6633',
               help=("L3 Controler IP list list tcp:ip_addr:port;"
                     "tcp:ip_addr:port..;..")),
    cfg.StrOpt('net_controller_l3_southbound_protocol',
               default='OpenFlow',
               help=("Southbound protocol to connect the forwarding"
                     "element Currently supports only OpenFlow"))
]

cfg.CONF.register_opts(NET_CONTROL_L3_OPTS)


L3_SDN_AGNET_TYPE = "SDN_app_l3"


class ControllerL3ServicePlugin(common_db_mixin.CommonDbMixin,
                                l3_gwmode_db.L3_NAT_db_mixin,
                                l3_hascheduler_db.L3_HA_scheduler_db_mixin):

    RPC_API_VERSION = '1.2'
    supported_extension_aliases = ["router", "ext-gw-mode"]

    def __init__(self):

        self.setup_rpc()
        self.router_scheduler = importutils.import_object(
            cfg.CONF.router_scheduler_driver)
        self.start_periodic_agent_status_check()
        if cfg.CONF.net_controller_l3_southbound_protocol == "OpenFlow":
            # Open Flow Controller
            LOG.debug(("Using Southbound OpenFlow Protocol "))
            self.controllerThread = ControllerRunner("openflow")
            self.controllerThread.start()
            self.controllerThread.router_scheduler = self.router_scheduler
            self.controllerThread.endpoints = self.endpoints

        elif cfg.CONF.net_controller_l3_southbound_protocol == "OVSDB":
            LOG.error(("Southbound OVSDB Protocol not implemented yet"))
        elif cfg.CONF.net_controller_l3_southbound_protocol == "OP-FLEX":
            LOG.error(("Southbound OP-FLEX Protocol not implemented yet"))

        super(ControllerL3ServicePlugin, self).__init__()

    def setup_rpc(self):
        # RPC support
        self.topic = topics.L3PLUGIN
        self.conn = n_rpc.create_connection(new=True)
        self.agent_notifiers.update(
            {q_const.AGENT_TYPE_L3: l3_rpc_agent_api.L3AgentNotifyAPI()})
        self.endpoints = [l3_rpc.L3RpcCallback()]
        self.conn.create_consumer(self.topic, self.endpoints,
                                  fanout=True)
        self.conn.consume_in_threads()

    def get_plugin_type(self):
        return constants.L3_ROUTER_NAT

    def get_plugin_description(self):
        """Returns string description of the plugin."""
        return "Net Controler Plugin reactive mode l3 Implementation"

    def create_floatingip(self, _context, floatingip):
        """Create floating IP.

        :param _context: Neutron request context
        :param floatingip: data for the floating IP being created
        :returns: A floating IP object on success

        """
        return super(ControllerL3ServicePlugin, self).create_floatingip(
            _context, floatingip,
            initial_status=q_const.FLOATINGIP_STATUS_DOWN)

    def add_router_interface_postcommit(self, _context, router_id,
                                        interface_info):
        # Update router's state first
        LOG.debug(("add_router_interface_postcommit "))
        self.controllerThread.bind_unscheduled_routers()
        self.controllerThread.update_device_up_by_port_id(
            interface_info['port_id'])
        # TODO(gampel) Add router info to Local datastore and abstruction layer
        # sync local data
        self.controllerThread.l3_r_app.notify_sync()

    def remove_router_interface_precommit(self, _context, router_id,
                                          interface_info):
        LOG.debug(("remove_router_interface_precommit"))
        # TODO(gampel) Add router info to Local datastore and abstruction layer

    def delete_router_precommit(self, _context, router_id):
        LOG.debug(("delete_router_precommit "))

    def update_router_postcommit(self, _context, router):
        self.controllerThread.bind_unscheduled_routers()
        LOG.debug(("update_router_postcommit "))
        if router['admin_state_up']:
            LOG.debug(("update_router_postcommit admin state up Enable "))
        else:
            LOG.debug(("update_router_postcommit admin state down disable"))

        self.controllerThread.l3_r_app.notify_sync()

    # Router API

    def create_router(self, *args, **kwargs):
        self.controllerThread.l3_r_app.create_router(self, *args, **kwargs)
        return super(ControllerL3ServicePlugin, self).create_router(
            *args, **kwargs)

    def update_router(self, _context, r_id, router):

        result = super(ControllerL3ServicePlugin, self).update_router(_context,
                                                                      r_id,
                                                                      router)
        self.update_router_postcommit(_context, result)
        return result

    def delete_router(self, _context, router_id):
        self.delete_router_precommit(_context, router_id)
        result = super(ControllerL3ServicePlugin, self).delete_router(_context,
                                                                    router_id)
        self.controllerThread.l3_r_app.notify_sync()
        return result

    # Router Interface API

    def add_router_interface(self, _context, router_id, interface_info):
        # Create interface in parent
        result = super(ControllerL3ServicePlugin, self).add_router_interface(
            _context, router_id, interface_info)
        try:
            self.add_router_interface_postcommit(_context, router_id,
                                                 result)
        except Exception:
            with excutils.save_and_reraise_exception():
                # Rollback db operation
                super(ControllerL3ServicePlugin, self).remove_router_interface(
                    _context, router_id, interface_info)
                return result

    def remove_router_interface(self, _context, router_id, interface_info):
        self.remove_router_interface_precommit(_context, router_id,
                                               interface_info)
        res = super(ControllerL3ServicePlugin, self).remove_router_interface(
            _context, router_id, interface_info)
        self.controllerThread.l3_r_app.notify_sync()
        return res

    def setup_vrouter_arp_responder(self, _context, br, action, table_id,
                                    segmentation_id, net_uuid, mac_address,
                                    ip_address):

        topic_port_update = topics.get_topic_name(topics.AGENT,
                                                  topics.PORT,
                                                  topics.UPDATE)
        target = messaging.Target(topic=topic_port_update)
        rpcapi = n_rpc.get_client(target)
        rpcapi.cast(_context,
                    'setup_entry_for_arp_reply_remote',
                    br_id="br-int",
                    action=action,
                    table_id=table_id,
                    segmentation_id=segmentation_id,
                    net_uuid=net_uuid,
                    mac_address=mac_address,
                    ip_address=ip_address)

    def update_agent_port_mapping_done(
            self, _context, agent_id, ip_address, host=None):
        LOG.debug(("::agent agent  <%s> on ip <%s> host <%s>  "),
                  agent_id,
                  ip_address,
                  host)
        self.send_set_controllers_update(_context, False)

    def send_set_controllers_update(self, _context, force_reconnect):

        topic_port_update = topics.get_topic_name(topics.AGENT,
                                                  topics.PORT,
                                                  topics.UPDATE)
        target = messaging.Target(topic=topic_port_update)
        rpcapi = n_rpc.get_client(target)
        iplist = cfg.CONF.L3controller_ip_list
        rpcapi.cast(_context,
                    'set_controller_for_br',
                    br_id="br-int",
                    ip_address_list=iplist,
                    force_reconnect=force_reconnect,
                    protocols="OpenFlow13")


class ControllerRunner(threading.Thread):

    def __init__(self, controllertype):
        super(ControllerRunner, self).__init__()
        self.controllertype = controllertype
        self.ctx = context.get_admin_context()
        self.hostname = utils.get_hostname()
        self.agent_state = {
            'binary': 'neutron-l3-agent',
            'host': self.hostname,
            'topic': topics.L3_AGENT,
            'configurations': {
                'agent_mode': 'legacy',
                'use_namespaces': True,
                'router_id': 1,
                'handle_internal_only_routers': True,
                'external_network_bridge': 'br-ex',
                'gateway_external_network_id': '',
                'interface_driver': "OpenFlow"},
            'start_flag': True,
            'agent_type': L3_SDN_AGNET_TYPE}
        self.l3_rpc = l3_rpc.L3RpcCallback()
        self.sync_active_state = False
        self.sync_all = True
        self.l3_r_app = None
        self.heartbeat = None
        self.open_flow_hand = None

    def start(self):
        app_mgr = AppManager.get_instance()
        LOG.debug(("running ryu openflow Controller lib  "))
        self.open_flow_hand = app_mgr.instantiate(OFPHandler, None, None)
        self.open_flow_hand.start()
        self.l3_r_app = app_mgr.instantiate(L3ReactiveApp, None, None)
        self.l3_r_app.start()
        ''' TODO fix this is hack to let the scheduler schedule the virtual
        router to L3 SDN app so this app will be in teh Agnet table as active
         Will be change when we convert this implementation to Service
         Plugin ----> l3 SDN agent for scalability Currently runs as tread
         will be converted to run as a standalone agent
        '''
        self.heartbeat = loopingcall.FixedIntervalLoopingCall(
            self._report_state_and_bind_routers)
        self.heartbeat.start(interval=30)

    def _report_state_and_bind_routers(self):
        if self.sync_all:
            l3plugin = manager.NeutronManager.get_service_plugins().get(
                constants.L3_ROUTER_NAT)
            l3plugin.send_set_controllers_update(self.ctx, True)
            self.sync_all = False
        plugin = manager.NeutronManager.get_plugin()
        plugin.create_or_update_agent(self.ctx, self.agent_state)
        self.bind_unscheduled_routers()
        if not self.sync_active_state:
            self.update_deviceup_on_all_vr_ports()
            self.sync_active_state = True

    def bind_unscheduled_routers(self):
        l3plugin = manager.NeutronManager.get_service_plugins().get(
            constants.L3_ROUTER_NAT)
        unscheduled_routers = []
        routers = l3plugin.get_routers(self.ctx, filters={})
        for router in routers:
            l3_agents = l3plugin.get_l3_agents_hosting_routers(
                self.ctx, [router['id']], admin_state_up=True)

            if l3_agents:
                LOG.debug(('Router %(router_id)s has already been '
                           'hosted by L3 agent %(agent_id)s'),
                          {'router_id': router['id'],
                           'agent_id': l3_agents[0]['id']})
            else:
                unscheduled_routers.append(router)

        if unscheduled_routers:

            l3_agent = l3plugin.get_enabled_agent_on_host(
                self.ctx, L3_SDN_AGNET_TYPE, utils.get_hostname())
            if l3_agent:
                self.router_scheduler.bind_routers(
                    self.ctx, l3plugin, unscheduled_routers, l3_agent)
                LOG.debug('Router %(router_id)s scheduled '
                          'to L3 SDN agent %(agent_id)s.',
                          {'agent_id': l3_agent.id,
                           'router_id': unscheduled_routers})
                # Update Port binbding

                self.l3_rpc._ensure_host_set_on_ports(
                    self.ctx, utils.get_hostname(), routers)
            else:
                LOG.error(("could not find fake l3 agent for L3 SDN app can"
                          "not schedule router id %(router_ids)s"),
                          {'router_ids': unscheduled_routers})

    def update_deviceup_on_all_vr_ports(self):

        l3plugin = manager.NeutronManager.get_service_plugins().get(
            constants.L3_ROUTER_NAT)
        routers = l3plugin.get_sync_data(self.ctx)
        for router in routers:
            for interface in router.get(q_const.INTERFACE_KEY, []):
                self.update_device_up(interface)

    def update_device_up_by_port_id(self, port_id):

        plugin = manager.NeutronManager.get_plugin()
        port = plugin._get_port(self.ctx, port_id)
        self.update_device_up(port)

    def update_device_up(self, port):
        plugin = manager.NeutronManager.get_plugin()
        #plugin.update_device_up(self.ctx, device)
        self.l3_rpc._ensure_host_set_on_port(
            self.ctx, utils.get_hostname(), port)
        plugin.update_port_status(self.ctx, port['id'],
                                  q_const.PORT_STATUS_ACTIVE,
                                  utils.get_hostname())
