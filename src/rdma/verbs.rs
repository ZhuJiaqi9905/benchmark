use rdma_sys::ibv_mtu::IBV_MTU_1024;
use rdma_sys::ibv_qp_state::IBV_QPS_INIT;
use rdma_sys::{
    _compat_ibv_port_attr, ibv_access_flags, ibv_ack_cq_events, ibv_alloc_pd, ibv_close_device,
    ibv_comp_channel, ibv_context, ibv_cq, ibv_create_comp_channel, ibv_create_cq, ibv_create_qp,
    ibv_dealloc_pd, ibv_dereg_mr, ibv_destroy_comp_channel, ibv_destroy_cq, ibv_destroy_qp,
    ibv_device, ibv_device_attr, ibv_free_device_list, ibv_get_cq_event, ibv_get_device_list,
    ibv_get_device_name, ibv_modify_qp, ibv_mr, ibv_open_device, ibv_pd, ibv_poll_cq,
    ibv_port_attr, ibv_post_recv, ibv_post_send, ibv_qp, ibv_qp_attr, ibv_qp_attr_mask,
    ibv_qp_init_attr, ibv_qp_state, ibv_qp_type, ibv_query_device, ibv_query_port, ibv_recv_wr,
    ibv_reg_mr, ibv_req_notify_cq, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_wc, ibv_wr_opcode,
    rdma_accept, rdma_ack_cm_event, rdma_bind_addr, rdma_cm_event, rdma_cm_event_type, rdma_cm_id,
    rdma_conn_param, rdma_connect, rdma_create_event_channel, rdma_create_id, rdma_create_qp,
    rdma_destroy_event_channel, rdma_destroy_id, rdma_destroy_qp, rdma_disconnect,
    rdma_event_channel, rdma_event_str, rdma_get_cm_event, rdma_get_local_addr, rdma_get_peer_addr,
    rdma_listen, rdma_migrate_id, rdma_port_space, rdma_reject, rdma_resolve_addr,
    rdma_resolve_route,
};
use std::ffi::{CStr, CString};
use std::net::SocketAddrV4;
use std::os::raw::{c_int, c_void};

#[derive(Clone)]
pub struct IbvPd {
    pub p_ibv_pd: *mut ibv_pd,
}

impl IbvPd {
    pub fn new_with_cm(cm_id: &RdmaCmId) -> Result<IbvPd, &'static str> {
        let pd = unsafe { ibv_alloc_pd((*cm_id.p_rdma_cm_id).verbs) };
        if pd.is_null() {
            return Err("alloc pd");
        }
        Ok(IbvPd { p_ibv_pd: pd })
    }
    pub fn new(context: &IbvContext) -> Result<IbvPd, &'static str> {
        let pd = unsafe { ibv_alloc_pd(context.p_ibv_context) };
        if pd.is_null() {
            return Err("ibv_alloc_pd()");
        }
        Ok(IbvPd { p_ibv_pd: pd })
    }
}

impl Drop for IbvPd {
    fn drop(&mut self) {
        let ret = unsafe { ibv_dealloc_pd(self.p_ibv_pd) };
        if ret != 0 {
            panic!("ibv_dealloc_pd() error");
        }
    }
}

#[derive(Clone)]
pub struct IbvContext {
    p_ibv_context: *mut ibv_context,
}

impl IbvContext {
    pub fn new(dev_name: Option<&str>) -> Result<IbvContext, &'static str> {
        let mut num_devs: c_int = 0;
        let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devs) };
        // if there isn't any IB device in host
        debug_assert_ne!(num_devs, 0, "found {} device(s)", num_devs);
        let ib_dev = match dev_name {
            None => unsafe { *dev_list_ptr },
            Some(dev_name) => {
                let dev_name_cstr = CString::new(dev_name).unwrap();
                let dev_list =
                    unsafe { std::slice::from_raw_parts(dev_list_ptr, num_devs as usize) };
                let mut tmp_dev = std::ptr::null_mut::<ibv_device>();
                for i in 0..(num_devs as usize) {
                    unsafe {
                        if libc::strcmp(ibv_get_device_name(dev_list[i]), dev_name_cstr.as_ptr())
                            == 0
                        {
                            tmp_dev = dev_list[i];
                            break;
                        }
                    }
                }
                tmp_dev
            }
        };
        // get device handle
        let context = unsafe { ibv_open_device(ib_dev) };
        if context.is_null() {
            unsafe { ibv_free_device_list(dev_list_ptr) };
            return Err("ibv_open_device()");
        }
        // free the device list
        unsafe { ibv_free_device_list(dev_list_ptr) };
        Ok(IbvContext {
            p_ibv_context: context,
        })
    }
    pub fn query_device(&self) -> ibv_device_attr {
        let mut device_attr = unsafe { std::mem::zeroed::<ibv_device_attr>() };
        let ret = unsafe { ibv_query_device(self.p_ibv_context, &mut device_attr) };
        if ret == -1 {
            panic!("ibv_query_device() error");
        }
        device_attr
    }
    pub fn get_lid(&self, port_num: u8) -> Result<u16, &'static str> {
        let mut port_attr = unsafe { std::mem::zeroed::<ibv_port_attr>() };
        let ret = unsafe {
            ibv_query_port(
                self.p_ibv_context,
                port_num,
                &mut port_attr as *mut _ as *mut _compat_ibv_port_attr,
            )
        };
        if ret == -1 {
            return Err("ibv_query_port()");
        }
        IBV_MTU_1024;
        Ok(port_attr.lid)
    }
}

impl Drop for IbvContext {
    fn drop(&mut self) {
        let ret = unsafe { ibv_close_device(self.p_ibv_context) };
        if ret == -1 {
            panic!("ibv_close_device() error");
        }
    }
}

#[derive(Clone)]
pub struct IbvMr {
    p_ibv_mr: *mut ibv_mr,
}

impl IbvMr {
    pub fn new(
        pd: &IbvPd,
        region: &[u8],
        access_flag: ibv_access_flags,
    ) -> Result<IbvMr, &'static str> {
        let mr = unsafe {
            ibv_reg_mr(
                pd.p_ibv_pd,
                region.as_ptr() as *mut c_void,
                region.len(),
                access_flag.0 as c_int,
            )
        };
        if mr.is_null() {
            return Err("ibv_reg_mr()");
        }
        Ok(IbvMr { p_ibv_mr: mr })
    }
    pub fn new_raw(
        pd: &IbvPd,
        region: *mut c_void,
        region_len: usize,
        access_flag: ibv_access_flags,
    ) -> Result<IbvMr, &'static str> {
        let mr = unsafe { ibv_reg_mr(pd.p_ibv_pd, region, region_len, access_flag.0 as c_int) };
        if mr.is_null() {
            return Err("ibv_reg_mr()");
        }
        Ok(IbvMr { p_ibv_mr: mr })
    }
    pub fn rkey(&self) -> u32 {
        return unsafe { (*self.p_ibv_mr).rkey };
    }
    pub fn lkey(&self) -> u32 {
        return unsafe { (*self.p_ibv_mr).lkey };
    }
}

impl Drop for IbvMr {
    fn drop(&mut self) {
        let ret = unsafe { ibv_dereg_mr(self.p_ibv_mr) };
        if ret == -1 {
            panic!("ibv_dereg_mr() error");
        }
    }
}

#[derive(Clone)]
pub struct RdmaEventChannel {
    p_rdma_event_channel: *mut rdma_event_channel,
    p_rdma_cm_event: *mut rdma_cm_event,
}

impl RdmaEventChannel {
    pub fn new() -> Result<RdmaEventChannel, &'static str> {
        let event_channel = unsafe { rdma_create_event_channel() };
        if event_channel.is_null() {
            return Err("create rdma_event_channel");
        }
        let cm_event = std::ptr::null_mut::<rdma_cm_event>();
        Ok(RdmaEventChannel {
            p_rdma_event_channel: event_channel,
            p_rdma_cm_event: cm_event,
        })
    }
    pub fn get_and_ack_cm_event(&self) -> Result<rdma_cm_event, &'static str> {
        let mut p_rdma_cm_event = std::ptr::null_mut::<rdma_cm_event>();
        let mut ret = unsafe {
            rdma_get_cm_event(
                self.p_rdma_event_channel,
                &mut p_rdma_cm_event as *mut *mut _,
            )
        };
        if ret == -1 {
            return Err("rdma_get_cm_event");
        }
        let mut event_copy = unsafe { std::mem::zeroed::<rdma_cm_event>() };
        unsafe {
            libc::memcpy(
                &mut event_copy as *mut _ as *mut c_void,
                p_rdma_cm_event as *mut c_void,
                std::mem::size_of::<rdma_cm_event>(),
            );
        }
        ret = unsafe { rdma_ack_cm_event(p_rdma_cm_event as *mut _) };
        if ret == -1 {
            return Err("rdma_ack_cm_event");
        }
        Ok(event_copy)
    }
    // blocking function
    pub fn get_cm_event(&mut self) -> rdma_cm_event {
        let ret = unsafe {
            rdma_get_cm_event(
                self.p_rdma_event_channel,
                &mut self.p_rdma_cm_event as *mut *mut _,
            )
        };
        if ret == -1 {
            panic!("rdma_get_cm_event() error");
        }
        let mut event_copy = unsafe { std::mem::zeroed::<rdma_cm_event>() };
        unsafe {
            libc::memcpy(
                &mut event_copy as *mut _ as *mut c_void,
                self.p_rdma_cm_event as *mut c_void,
                std::mem::size_of::<rdma_cm_event>(),
            );
        }
        event_copy
    }

    pub fn ack_cm_event(&self) {
        let ret = unsafe { rdma_ack_cm_event(self.p_rdma_cm_event as *mut _) };
        if ret == -1 {
            panic!("rdma_ack_cm_event() error");
        }
    }
}

impl Drop for RdmaEventChannel {
    fn drop(&mut self) {
        unsafe {
            rdma_destroy_event_channel(self.p_rdma_event_channel);
        };
    }
}

#[derive(Clone)]
pub struct RdmaCmId {
    p_rdma_cm_id: *mut rdma_cm_id,
}

impl RdmaCmId {
    pub fn new(
        event_channel: &RdmaEventChannel,
        ps: rdma_port_space::Type,
    ) -> Result<RdmaCmId, &'static str> {
        let mut cm_id = std::ptr::null_mut::<rdma_cm_id>();
        let ret = unsafe {
            rdma_create_id(
                event_channel.p_rdma_event_channel,
                &mut cm_id as *mut *mut _,
                std::ptr::null_mut::<c_void>(),
                ps,
            )
        };
        if ret == -1 {
            return Err("rdma_create_id()");
        }
        Ok(RdmaCmId {
            p_rdma_cm_id: cm_id,
        })
    }
    pub fn new_from_event(event: &rdma_cm_event) -> RdmaCmId {
        RdmaCmId {
            p_rdma_cm_id: event.id,
        }
    }
    pub fn connect(&self, conn_param: Option<&mut rdma_conn_param>) -> Result<(), &'static str> {
        let ret = match conn_param {
            None => unsafe { rdma_connect(self.p_rdma_cm_id, std::ptr::null_mut()) },
            Some(param) => unsafe { rdma_connect(self.p_rdma_cm_id, param as *mut _) },
        };
        if ret == -1 {
            return Err("rdma_connect()");
        }
        Ok(())
    }

    pub fn disconnect(&self) -> Result<(), &'static str> {
        let ret = unsafe { rdma_disconnect(self.p_rdma_cm_id) };
        if ret == -1 {
            return Err("rdma_disconnect()");
        }
        Ok(())
    }

    pub fn resolve_addr(
        &self,
        src_addr: &SocketAddrV4,
        dst_addr: &SocketAddrV4,
        timeout_ms: i32,
    ) -> Result<(), &'static str> {
        let mut src_addr_in = unsafe { new_sockaddr_in(src_addr) };
        let mut dst_addr_in = unsafe { new_sockaddr_in(dst_addr) };
        let ret = unsafe {
            rdma_resolve_addr(
                self.p_rdma_cm_id,
                &mut src_addr_in as *mut libc::sockaddr_in as *mut libc::sockaddr,
                &mut dst_addr_in as *mut libc::sockaddr_in as *mut libc::sockaddr,
                timeout_ms,
            )
        };
        if ret == -1 {
            return Err("rdma_resolve_addr()");
        }
        Ok(())
    }

    pub fn resolve_route(&self, timeout_ms: i32) -> Result<(), &'static str> {
        let ret = unsafe { rdma_resolve_route(self.p_rdma_cm_id, timeout_ms) };
        if ret == -1 {
            return Err("rdma_resolve_route()");
        }
        Ok(())
    }

    pub fn migrate_id(&self, event_channel: &RdmaEventChannel) -> Result<(), &'static str> {
        let ret = unsafe { rdma_migrate_id(self.p_rdma_cm_id, event_channel.p_rdma_event_channel) };
        if ret == -1 {
            return Err("rdma_migrate_id()");
        }
        Ok(())
    }

    pub fn bind_addr(&self, listen_addr: &SocketAddrV4) -> Result<(), &'static str> {
        let mut addr = unsafe { new_sockaddr_in(listen_addr) };
        let ret = unsafe {
            rdma_bind_addr(
                self.p_rdma_cm_id,
                &mut addr as *mut libc::sockaddr_in as *mut _,
            )
        };
        if ret == -1 {
            return Err("rdma_bind_addr()");
        }
        Ok(())
    }

    pub fn listen(&self, backlog: i32) -> Result<(), &'static str> {
        let ret = unsafe { rdma_listen(self.p_rdma_cm_id, backlog) };
        if ret == -1 {
            return Err("rdma_listen()");
        }
        Ok(())
    }

    pub fn accept(&self, conn_param: Option<&mut rdma_conn_param>) -> Result<(), &'static str> {
        let ret = match conn_param {
            None => unsafe { rdma_accept(self.p_rdma_cm_id, std::ptr::null_mut()) },
            Some(param) => unsafe { rdma_accept(self.p_rdma_cm_id, param as *mut _) },
        };
        if ret == -1 {
            return Err("rdma_accept()");
        }
        Ok(())
    }

    pub fn reject(&self, data: &[u8]) {
        let ret = unsafe {
            rdma_reject(
                self.p_rdma_cm_id,
                data.as_ptr() as *const c_void,
                data.len() as u8,
            )
        };
        if ret == -1 {
            panic!("rdma_reject() error");
        }
    }

    pub fn get_local_addr(&self) -> &libc::sockaddr {
        unsafe { rdma_get_local_addr(&(*self.p_rdma_cm_id)) }
    }

    pub fn get_peer_addr(&self) -> &libc::sockaddr {
        unsafe { rdma_get_peer_addr(&(*self.p_rdma_cm_id)) }
    }

    pub fn create_qp(
        &self,
        pd: &IbvPd,
        send_cq: &IbvCq,
        recv_cq: &IbvCq,
        sq_sig_all: i32,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_send_sge: u32,
        max_recv_sge: u32,
    ) -> Result<IbvQp, &'static str> {
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
        qp_init_attr.sq_sig_all = sq_sig_all; // set to 0 to avoid CQE for every SR
        qp_init_attr.send_cq = send_cq.p_ibv_cq;
        qp_init_attr.recv_cq = recv_cq.p_ibv_cq;
        qp_init_attr.cap.max_send_wr = max_send_wr;
        qp_init_attr.cap.max_recv_wr = max_recv_wr;
        qp_init_attr.cap.max_send_sge = max_send_sge;
        qp_init_attr.cap.max_recv_sge = max_recv_sge;
        let ret =
            unsafe { rdma_create_qp(self.p_rdma_cm_id, pd.p_ibv_pd, &mut qp_init_attr as *mut _) };
        if ret == -1 {
            return Err("rdma_create_qp()");
        }
        unsafe {
            Ok(IbvQp {
                p_ibv_qp: (*self.p_rdma_cm_id).qp,
                p_rdma_cm_id: self.p_rdma_cm_id,
            })
        }
    }
}

impl Drop for RdmaCmId {
    fn drop(&mut self) {
        let ret = unsafe { rdma_destroy_id(self.p_rdma_cm_id) };
        if ret == -1 {
            panic!("rdma_destroy_id() error");
        }
    }
}

pub fn get_event_str(cm_event_type: rdma_cm_event_type::Type) -> &'static str {
    let ret = unsafe { CStr::from_ptr(rdma_event_str(cm_event_type)) };
    ret.to_str().unwrap()
}

#[derive(Clone)]
pub struct IbvCq {
    p_ibv_cq: *mut ibv_cq,
}

impl IbvCq {
    pub fn new(context: &IbvContext, cqe: i32) -> Result<IbvCq, &'static str> {
        let cq = unsafe {
            ibv_create_cq(
                context.p_ibv_context,
                cqe,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if cq.is_null() {
            return Err("ibv_create_cq()");
        }
        Ok(IbvCq { p_ibv_cq: cq })
    }
    pub fn new_with_cm(cm_id: &RdmaCmId, cqe: i32) -> Result<IbvCq, &'static str> {
        let cq = unsafe {
            ibv_create_cq(
                (*cm_id.p_rdma_cm_id).verbs,
                cqe,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };
        if cq.is_null() {
            return Err("ibv_create_cq()");
        }
        Ok(IbvCq { p_ibv_cq: cq })
    }

    pub fn poll<'a>(&self, cqe_arr: &'a mut [ibv_wc]) -> &'a [ibv_wc] {
        let n = unsafe { ibv_poll_cq(self.p_ibv_cq, cqe_arr.len() as i32, cqe_arr.as_mut_ptr()) };
        if n < 0 {
            panic!("ibv_poll_cq() error");
        }
        &mut cqe_arr[0..n as usize]
    }
}

impl Drop for IbvCq {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_cq(self.p_ibv_cq) };
        if ret == -1 {
            panic!("ibv_destroy_cq error");
        }
    }
}

#[derive(Clone)]
pub struct IbvCompChannel {
    p_ibv_comp_channel: *mut ibv_comp_channel,
}

impl IbvCompChannel {
    pub fn new(context: &mut IbvContext) -> IbvCompChannel {
        let comp_channel = unsafe { ibv_create_comp_channel(context.p_ibv_context) };
        if comp_channel.is_null() {
            panic!("ibv_create_comp_channel() error");
        }
        IbvCompChannel {
            p_ibv_comp_channel: comp_channel,
        }
    }

    pub fn req_notify_cq(&self, cq: &IbvCq, solicited_only: i32) {
        let ret = unsafe { ibv_req_notify_cq(cq.p_ibv_cq, solicited_only) };
        if ret == -1 {
            panic!("ibv_req_notify_cq() error");
        }
    }
    // 这个不容易封装。先不用IbvCompChannel吧。之后可以考虑如果使用IbvCompChannel后，就在内部维护IbvCq
    pub fn get_cq_event(&self) -> *mut ibv_cq {
        let mut cq = std::ptr::null_mut::<ibv_cq>();
        let mut cq_context = std::ptr::null_mut::<c_void>();
        unsafe {
            ibv_get_cq_event(
                self.p_ibv_comp_channel,
                &mut cq as *mut *mut _,
                &mut cq_context as *mut *mut _,
            );
        }
        cq
    }

    pub fn ack_cq_events(&self, cq: *mut ibv_cq, nevents: u32) {
        unsafe {
            ibv_ack_cq_events(cq, nevents);
        }
    }
}

impl Drop for IbvCompChannel {
    fn drop(&mut self) {
        let ret = unsafe { ibv_destroy_comp_channel(self.p_ibv_comp_channel) };
        if ret == -1 {
            panic!("ibv_destroy_comp_channel() error");
        }
    }
}

#[derive(Clone)]
pub struct IbvQp {
    p_ibv_qp: *mut ibv_qp,
    p_rdma_cm_id: *mut rdma_cm_id,
}
impl IbvQp {
    pub fn new(
        pd: &IbvPd,
        send_cq: &IbvCq,
        recv_cq: &IbvCq,
        sq_sig_all: i32,
        max_send_wr: u32,
        max_recv_wr: u32,
        max_send_sge: u32,
        max_recv_sge: u32,
        max_inline_data: u32,
    ) -> Result<IbvQp, &'static str> {
        let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        qp_init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
        qp_init_attr.sq_sig_all = sq_sig_all; // set to 0 to avoid CQE for every SR
        qp_init_attr.send_cq = send_cq.p_ibv_cq;
        qp_init_attr.recv_cq = recv_cq.p_ibv_cq;
        qp_init_attr.cap.max_send_wr = max_send_wr;
        qp_init_attr.cap.max_recv_wr = max_recv_wr;
        qp_init_attr.cap.max_send_sge = max_send_sge;
        qp_init_attr.cap.max_recv_sge = max_recv_sge;
        qp_init_attr.cap.max_inline_data = max_inline_data;
        let p_ibv_qp = unsafe { ibv_create_qp(pd.p_ibv_pd, &mut qp_init_attr as *mut _) };
        if p_ibv_qp.is_null() {
            return Err("ibv_create_qp()");
        }

        Ok(IbvQp {
            p_ibv_qp,
            p_rdma_cm_id: std::ptr::null_mut(),
        })
    }
    pub fn modify_reset2init(&self, port_num: u8) -> Result<(), &'static str> {
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = port_num;
        qp_attr.qp_access_flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
        let ret = unsafe {
            ibv_modify_qp(
                self.p_ibv_qp,
                &mut qp_attr as *mut _,
                (ibv_qp_attr_mask::IBV_QP_STATE.0
                    | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0
                    | ibv_qp_attr_mask::IBV_QP_PORT.0
                    | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0) as i32,
            )
        };
        if ret == -1 {
            return Err("ibv_modify_qp()");
        }
        Ok(())
    }
    pub fn modify_init2rtr(
        &self,

        sl: u8,
        port_num: u8,
        remote_qpn: u32,
        remote_psn: u32,
        remote_lid: u16,
    ) -> Result<(), &'static str> {
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        qp_attr.path_mtu = IBV_MTU_1024;
        qp_attr.dest_qp_num = remote_qpn;
        qp_attr.rq_psn = remote_psn;
        qp_attr.max_dest_rd_atomic = 1;
        qp_attr.min_rnr_timer = 12;
        qp_attr.ah_attr.is_global = 0;
        qp_attr.ah_attr.dlid = remote_lid;
        qp_attr.ah_attr.sl = sl;
        qp_attr.ah_attr.src_path_bits = 0;
        qp_attr.ah_attr.port_num = port_num;
        let ret = unsafe {
            ibv_modify_qp(
                self.p_ibv_qp,
                &mut qp_attr as *mut _,
                (ibv_qp_attr_mask::IBV_QP_STATE.0
                    | ibv_qp_attr_mask::IBV_QP_AV.0
                    | ibv_qp_attr_mask::IBV_QP_PATH_MTU.0
                    | ibv_qp_attr_mask::IBV_QP_DEST_QPN.0
                    | ibv_qp_attr_mask::IBV_QP_RQ_PSN.0
                    | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC.0
                    | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER.0) as i32,
            )
        };
        if ret == -1 {
            return Err("ibv_modify_qp()");
        }
        Ok(())
    }

    pub fn modify_rtr2rts(&self, psn: u32) -> Result<(), &'static str> {
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        qp_attr.timeout = 14;
        qp_attr.retry_cnt = 7;
        qp_attr.rnr_retry = 7;
        qp_attr.sq_psn = psn;
        qp_attr.max_rd_atomic = 1;
        let ret = unsafe {
            ibv_modify_qp(
                self.p_ibv_qp,
                &mut qp_attr as *mut _,
                (ibv_qp_attr_mask::IBV_QP_STATE.0
                    | ibv_qp_attr_mask::IBV_QP_TIMEOUT.0
                    | ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0
                    | ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0
                    | ibv_qp_attr_mask::IBV_QP_SQ_PSN.0
                    | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0) as i32,
            )
        };
        if ret == -1 {
            return Err("ibv_modify_qp()");
        }
        Ok(())
    }
    pub fn get_qpn(&self) -> u32 {
        unsafe { (*self.p_ibv_qp).qp_num }
    }
}
impl Drop for IbvQp {
    fn drop(&mut self) {
        if self.p_rdma_cm_id.is_null() {
            let ret = unsafe { ibv_destroy_qp(self.p_ibv_qp) };
            if ret == -1 {
                panic!("ibv_destroy_qp() error");
            }
        } else {
            unsafe {
                rdma_destroy_qp(self.p_rdma_cm_id);
            }
        };
    }
}

pub fn post_send(buffer: &[u8], qp: &IbvQp, lkey: u32, wr_id: u64) {
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
    let mut sge = ibv_sge {
        addr: buffer.as_ptr() as u64,
        length: buffer.len() as u32,
        lkey,
    };
    let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };

    wr.wr_id = wr_id;
    wr.next = std::ptr::null_mut::<ibv_send_wr>() as *mut _;
    wr.sg_list = &mut sge as *mut _;
    wr.num_sge = 1;
    wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
    let ret = unsafe { ibv_post_send(qp.p_ibv_qp, &mut wr as *mut _, &mut bad_wr as *mut _) };
    if ret == -1 {
        panic!("post_send() error");
    }
}

pub fn post_recv(buffer: &[u8], qp: &IbvQp, lkey: u32, wr_id: u64) {
    let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();
    let mut sge = ibv_sge {
        addr: buffer.as_ptr() as u64,
        length: buffer.len() as u32,
        lkey,
    };
    let mut wr = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
    wr.wr_id = wr_id;
    wr.next = std::ptr::null_mut();
    wr.sg_list = &mut sge as *mut _;
    wr.num_sge = 1;
    let ret = unsafe { ibv_post_recv(qp.p_ibv_qp, &mut wr as *mut _, &mut bad_wr as *mut _) };
    if ret == -1 {
        panic!("post_recv() error");
    }
}

pub fn post_read(
    buffer: &[u8],
    remote_addr: u64,
    rkey: u32,
    qp: &IbvQp,
    mr: &IbvMr,
    wr_id: u64,
    send_flags: u32,
) {
    let ret = post_read_or_write(
        buffer,
        remote_addr,
        rkey,
        qp,
        mr,
        wr_id,
        send_flags,
        ibv_wr_opcode::IBV_WR_RDMA_READ,
    );
    if ret == -1 {
        panic!("post_write() error");
    }
}

pub fn post_write(
    buffer: &[u8],
    remote_addr: u64,
    rkey: u32,
    qp: &IbvQp,
    mr: &IbvMr,
    wr_id: u64,
    send_flags: u32,
) {
    let ret = post_read_or_write(
        buffer,
        remote_addr,
        rkey,
        qp,
        mr,
        wr_id,
        send_flags,
        ibv_wr_opcode::IBV_WR_RDMA_WRITE,
    );
    if ret == -1 {
        panic!("post_write() error");
    }
}

fn post_read_or_write(
    buffer: &[u8],
    remote_addr: u64,
    rkey: u32,
    qp: &IbvQp,
    mr: &IbvMr,
    wr_id: u64,
    send_flags: u32,
    opcode: ibv_wr_opcode::Type,
) -> i32 {
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
    let mut sge = unsafe {
        ibv_sge {
            addr: buffer.as_ptr() as u64,
            length: buffer.len() as u32,
            lkey: (*mr.p_ibv_mr).lkey,
        }
    };
    let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    wr.wr_id = wr_id;
    wr.next = std::ptr::null_mut::<ibv_send_wr>() as *mut _;
    wr.sg_list = &mut sge as *mut _;
    wr.num_sge = 1;
    wr.opcode = opcode;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;
    wr.send_flags = send_flags;
    unsafe { ibv_post_send(qp.p_ibv_qp, &mut wr as *mut _, &mut bad_wr as *mut _) }
}

unsafe fn new_sockaddr_in(addr: &SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as u16,
        sin_port: addr.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_be_bytes(addr.ip().octets()).to_be(),
        },
        sin_zero: std::mem::zeroed(),
    }
}
