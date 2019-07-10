/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, ChangeDetectionStrategy, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { combineLatest, Subject } from 'rxjs';
import { map, takeUntil } from 'rxjs/operators';
import { JobService } from 'services';
import { trigger, animate, style, transition } from '@angular/animations';

@Component({
  selector: 'flink-job-overview-drawer',
  templateUrl: './job-overview-drawer.component.html',
  styleUrls: ['./job-overview-drawer.component.less'],
  animations: [
    trigger('drawer', [
      transition('void => *', [
        style({ transform: 'translateX(100%)' }),
        animate(100, style({ transform: 'translateX(0)' }))
      ]),
      transition('* => void', [
        style({ transform: 'translateX(0)' }),
        animate(100, style({ transform: 'translateX(100%)' }))
      ])
    ])
  ],
  host: {
    '[@drawer]': '',
    '[class.full-width]': 'fullScreen'
  },
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class JobOverviewDrawerComponent implements OnInit, OnDestroy {
  listOfNavigation = this.jobService.listOfNavigation;
  fullScreen = false;
  private cachePath = this.listOfNavigation[0].path;
  private destroy$ = new Subject();

  closeDrawer() {
    if (this.fullScreen) {
      this.fullScreen = false;
    } else {
      this.router.navigate(['../../'], { relativeTo: this.activatedRoute, queryParamsHandling: 'merge' }).then();
    }
  }

  fullDrawer() {
    this.fullScreen = true;
  }

  constructor(private activatedRoute: ActivatedRoute, private router: Router, private jobService: JobService) {}

  ngOnInit() {
    const nodeId$ = this.activatedRoute.params.pipe(map(item => item.vertexId));
    combineLatest([this.jobService.jobDetail$.pipe(map(item => item.plan.nodes)), nodeId$])
      .pipe(takeUntil(this.destroy$))
      .subscribe(data => {
        const [nodes, nodeId] = data;
        if (!this.activatedRoute.firstChild) {
          this.router.navigate([this.cachePath], { relativeTo: this.activatedRoute, queryParamsHandling: 'merge' });
        } else {
          this.cachePath = this.activatedRoute.firstChild.snapshot.data.path;
        }
        if (nodes && nodeId) {
          this.jobService.selectedVertex$.next(nodes.find(item => item.id === nodeId));
        }
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.jobService.selectedVertex$.next(null);
  }
}
