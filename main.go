package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xanzy/go-gitlab"
)

var (
	targetGitLab      = "https://git.target-gitlab-server.com"
	targetToken       = "xxxxxxxxxxxx" // Target GitLab token
	sourceGitLab      = "https://git.gitlab-source-server.com"
	sourceGitLabToken = "xxxxxxxxxxxxxxxx" // Source GitLab token
	targetNamespace   = "ramooz"
)

type targetGroupSt struct {
	ID    int
	Group *gitlab.Group
}

func main() {
	// Configuration

	// Create clients
	sourceClient, err := gitlab.NewClient(sourceGitLabToken, gitlab.WithBaseURL(sourceGitLab))
	if err != nil {
		log.Fatalf("Failed to create source client: %v", err)
	}

	targetClient, err := gitlab.NewClient(targetToken, gitlab.WithBaseURL(targetGitLab))
	if err != nil {
		log.Fatalf("Failed to create target client: %v", err)
	}

	// Verify target namespace is a group
	ramoozTargetGroup, err := createOrGetGroup(targetClient, targetNamespace)
	if err != nil {
		log.Fatalf("Failed to create/get target group: %v", err)
	}
	fmt.Printf("Target group created/retrieved: %s (ID: %d)\n", ramoozTargetGroup.FullPath, ramoozTargetGroup.ID)

	// Get all source groups (including subgroups)
	sourceGroups, err := getAllGroups(sourceClient)
	if err != nil {
		log.Fatalf("Failed to get source groups: %v", err)
	}

	// Create a map to track source to target group IDs
	groupMap := make(map[int]*targetGroupSt)
	groupMap[0] = &targetGroupSt{ID: ramoozTargetGroup.ID, Group: ramoozTargetGroup} // Root maps to our target group
	createAllGroupsInTarget(groupMap, sourceGroups, targetClient, 0)

	// Get all projects from source and import to target
	for _, sourceGroup := range sourceGroups {
		// Skip personal namespaces
		if isPersonalNamespace(sourceGroup) {
			continue
		}

		targetGroup, ok := groupMap[sourceGroup.ID]
		if !ok {
			continue
		}

		projects, err := getGroupProjects(sourceClient, sourceGroup.ID)
		if err != nil {
			fmt.Printf("Failed to get projects for group %s: %v\n", sourceGroup.FullPath, err)
			continue
		}

		for _, project := range projects {
			err := importProject(targetClient, targetToken, targetGitLab, sourceClient, project, targetGroup.ID)
			if err != nil {
				fmt.Printf("Failed to import project %s: %v\n", project.PathWithNamespace, err)
				continue
			}
			fmt.Printf("Successfully imported project: %s to group %s\n", project.Name, targetGroup.Group.FullPath)
		}
	}
}

func createAllGroupsInTarget(groupMap map[int]*targetGroupSt, sourceGroups []*gitlab.Group, targetClient *gitlab.Client, parentId int) {
	// Create all groups in target
	for _, sourceGroup := range sourceGroups {
		if sourceGroup.ParentID != parentId {
			continue
		}
		// Skip personal namespaces
		if isPersonalNamespace(sourceGroup) {
			fmt.Printf("Skipping personal namespace: %s\n", sourceGroup.FullPath)
			continue
		}

		targetParent, ok := groupMap[sourceGroup.ParentID]
		if !ok {
			fmt.Printf("Parent group not found for %s (targetParent ID: %d), skipping\n", sourceGroup.FullPath, sourceGroup.ParentID)
			continue
		}
		fullPath := sourceGroup.FullPath
		if strings.HasPrefix(fullPath, "ramooz/") {
			fullPath = fullPath[7:]
		}
		if fullPath == "ramooz" {
			groupMap[sourceGroup.ID] = groupMap[0]
			createAllGroupsInTarget(groupMap, sourceGroups, targetClient, sourceGroup.ID)
			continue
		}
		// Create the group in target
		targetGroup, err := createGroup(targetClient, sourceGroup, targetParent.ID)
		if err != nil {
			fmt.Printf("Failed to create group %s: %v\n", sourceGroup.FullPath, err)
			targetGroup, err = getGroup(targetClient, fmt.Sprintf("%s/%s", targetNamespace, fullPath))
			if err != nil {
				fmt.Printf("Failed to get group %s: %v\n", sourceGroup.FullPath, err)
				continue
			}
		}
		groupMap[sourceGroup.ID] = &targetGroupSt{targetGroup.ID, sourceGroup}
		fmt.Printf("Created group: %s (ID: %d) under targetParent ID %d\n", targetGroup.FullPath, targetGroup.ID, targetParent)
		createAllGroupsInTarget(groupMap, sourceGroups, targetClient, sourceGroup.ID)
	}
}
func importWithRetry(req *http.Request) (*http.Response, error) {
	maxRetries := 5
	delay := 10 * time.Second

	client := &http.Client{}
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("import failed (attempt %d): %w", attempt, err)
		}

		if resp.StatusCode == 429 {
			log.Printf("🚦 Rate limited on import (429). Waiting %v before retry %d...", delay, attempt)
			time.Sleep(delay)
			delay *= 2
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("too many 429 responses from import endpoint")
}

func isPersonalNamespace(group *gitlab.Group) bool {
	return false
}
func getGroup(client *gitlab.Client, groupPath string) (*gitlab.Group, error) {
	// Try to get the group first
	group, _, err := client.Groups.GetGroup(groupPath, nil)

	return group, err
}
func createOrGetGroup(client *gitlab.Client, groupPath string) (*gitlab.Group, error) {
	// Try to get the group first
	group, _, err := client.Groups.GetGroup(groupPath, nil)
	if err == nil {
		return group, nil
	}

	// If not found, create it
	parts := strings.Split(groupPath, "/")
	createOpt := &gitlab.CreateGroupOptions{
		Name:       gitlab.String(parts[len(parts)-1]),
		Path:       gitlab.String(parts[len(parts)-1]),
		Visibility: gitlab.Visibility(gitlab.PrivateVisibility),
	}

	if len(parts) > 1 {
		parentPath := strings.Join(parts[:len(parts)-1], "/")
		parentGroup, _, err := client.Groups.GetGroup(parentPath, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get parent group %s: %v", parentPath, err)
		}
		createOpt.ParentID = gitlab.Int(parentGroup.ID)
	}

	newGroup, _, err := client.Groups.CreateGroup(createOpt)
	if err != nil {
		return nil, fmt.Errorf("failed to create group %s: %v", groupPath, err)
	}

	return newGroup, nil
}

func getAllGroups(client *gitlab.Client) ([]*gitlab.Group, error) {
	var allGroups []*gitlab.Group
	opt := &gitlab.ListGroupsOptions{
		ListOptions: gitlab.ListOptions{
			PerPage: 100,
			Page:    1,
		},
		AllAvailable: gitlab.Bool(false),
	}

	for {
		groups, resp, err := client.Groups.ListGroups(opt)
		if err != nil {
			return nil, err
		}

		allGroups = append(allGroups, groups...)

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return allGroups, nil
}

func createGroup(client *gitlab.Client, sourceGroup *gitlab.Group, parentID int) (*gitlab.Group, error) {
	createOpt := &gitlab.CreateGroupOptions{
		Name:        gitlab.String(sourceGroup.Name),
		Path:        gitlab.String(sourceGroup.Path),
		Description: gitlab.String(sourceGroup.Description),
		Visibility:  gitlab.Visibility(gitlab.VisibilityValue(sourceGroup.Visibility)),
		ParentID:    gitlab.Int(parentID),
	}

	group, _, err := client.Groups.CreateGroup(createOpt)
	if err != nil {
		return nil, err
	}

	return group, nil
}

func getGroupProjects(client *gitlab.Client, groupID int) ([]*gitlab.Project, error) {
	var f *bool
	f = new(bool)
	*f = false
	var allProjects []*gitlab.Project
	opt := &gitlab.ListGroupProjectsOptions{
		ListOptions: gitlab.ListOptions{
			PerPage: 100,
			Page:    1,
		},
		IncludeSubGroups: f,
	}

	for {
		projects, resp, err := client.Groups.ListGroupProjects(groupID, opt)
		if err != nil {
			return nil, err
		}

		allProjects = append(allProjects, projects...)

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return allProjects, nil
}

func importProject(targetClient *gitlab.Client, targetToken, targetURL string, sourceClient *gitlab.Client, project *gitlab.Project, targetGroupID int) error {
	// Verify target is a group
	targetGroup, _, err := targetClient.Groups.GetGroup(targetGroupID, nil)
	if err != nil {
		return fmt.Errorf("target namespace %d is not a valid group: %v", targetGroupID, err)
	}

	// Schedule the export
	_, err = sourceClient.ProjectImportExport.ScheduleExport(project.ID, nil)
	if err != nil {
		return fmt.Errorf("failed to schedule export: %v", err)
	}

	// Wait for export to complete
	var exportStatus *gitlab.ExportStatus
	for {
		exportStatus, _, err = sourceClient.ProjectImportExport.ExportStatus(project.ID)
		if err != nil {
			return fmt.Errorf("failed to check export status: %v", err)
		}
		if exportStatus.ExportStatus == "finished" {
			break
		}
		time.Sleep(time.Second)
	}

	// Download export archive
	exportData, _, err := sourceClient.ProjectImportExport.ExportDownload(project.ID)
	if err != nil {
		return fmt.Errorf("failed to download export archive: %v", err)
	}

	// Save to temp file
	tempFile, err := os.CreateTemp("", "gitlab-export-*.tar.gz")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if _, err := tempFile.Write(exportData); err != nil {
		return fmt.Errorf("failed to write export to temp file: %v", err)
	}

	// Open again for reading
	file, err := os.Open(tempFile.Name())
	if err != nil {
		return fmt.Errorf("failed to open temp file for reading: %v", err)
	}
	defer file.Close()

	// Prepare multipart form data
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	_ = writer.WriteField("name", project.Name)
	_ = writer.WriteField("path", project.Path)
	_ = writer.WriteField("namespace", targetGroup.FullPath) // Use group path instead of ID

	part, err := writer.CreateFormFile("file", filepath.Base(tempFile.Name()))
	if err != nil {
		return fmt.Errorf("failed to create form file: %v", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("failed to copy file content: %v", err)
	}
	writer.Close()

	// Send import request
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v4/projects/import", targetURL), body)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("PRIVATE-TOKEN", targetToken)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := importWithRetry(req)
	if err != nil {
		return fmt.Errorf("failed to perform import request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("import failed: %s", respBody)
	}

	return nil
}
