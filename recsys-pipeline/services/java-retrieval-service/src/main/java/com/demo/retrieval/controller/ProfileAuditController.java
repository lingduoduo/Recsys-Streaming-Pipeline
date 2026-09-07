package com.demo.retrieval.controller;

import com.demo.retrieval.service.audit.ProfileAuditService;
import com.demo.retrieval.service.audit.ProfileAuditService.AuditBusyException;
import com.demo.retrieval.service.audit.ProfileAuditService.ProfileAuditFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Operator tool, like /actuator/model-reload: walks user → has_profile → preferences → catalog
 * content and returns a findings-only report. Records no serving measurement.
 */
@RestController
public class ProfileAuditController {
    private static final Logger log = LoggerFactory.getLogger(ProfileAuditController.class);

    private final ProfileAuditService auditService;

    public ProfileAuditController(ProfileAuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping("/actuator/profile-audit")
    public ResponseEntity<?> audit(@RequestParam(required = false) Integer limit) {
        try {
            return ResponseEntity.ok(auditService.audit(limit));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (AuditBusyException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", "busy"));
        } catch (ProfileAuditFailedException e) {
            log.error("Profile audit failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }
}
